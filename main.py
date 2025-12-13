from flask import Flask, render_template, request, url_for, jsonify, abort, redirect, session
from Cassandra import Cassandra
from datetime import datetime
import hashlib
import os
import re
import smtplib
from email.message import EmailMessage
import uuid
import requests
from rapidfuzz import fuzz

app = Flask(__name__)
app.secret_key = "replace-this-with-a-random-secret-key"

# PayPal configuration (use environment variables in real deployments)
PAYPAL_CLIENT_ID = os.environ.get("PAYPAL_CLIENT_ID", "")
PAYPAL_CLIENT_SECRET = os.environ.get("PAYPAL_CLIENT_SECRET", "")
PAYPAL_API_BASE = os.environ.get("PAYPAL_API_BASE", "https://api-m.sandbox.paypal.com")

# Contact configuration
CONTACT_RECIPIENT_EMAIL = os.environ.get("CONTACT_RECIPIENT_EMAIL", "chloenomura4@gmail.com")
CONTACT_PHONE = os.environ.get("CONTACT_PHONE", "(555) 555-1234")
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")


def get_or_create_cart_id() -> str:
    """
    Return the current cart_id (UUID string) for this browser session,
    creating one if it does not yet exist.
    """
    cart_id = session.get('cart_id')
    if not cart_id:
        cart_id = str(uuid.uuid4())
        session['cart_id'] = cart_id
    return cart_id


@app.context_processor
def inject_globals():
    """Make cart_count and basic user info available in all templates."""
    user = session.get('user') or {}
    cart_id = session.get('cart_id')
    cart_count = 0

    if cart_id:
        cassandra = Cassandra()
        try:
            cart_count = cassandra.get_cart_item_count(cart_id)
        finally:
            cassandra.shutdown()

    user_first_name = user.get("name")
    user_type = user.get("type")
    if user_first_name:
        cart_label = f"{user_first_name}'s cart:"
    else:
        cart_label = None

    login_error = session.pop("login_error", None)

    return {
        "cart_count": cart_count,
        "user_first_name": user_first_name,
        "user_type": user_type,
        "cart_label": cart_label,
        "login_error": login_error,
    }


@app.before_request
def redirect_anonymous_to_home():
    """
    If the user is not logged in (and has not chosen guest),
    and they try to access any page other than the home page,
    send them to the home page, where the login modal will appear.
    """
    # Allow these endpoints without redirect
    if request.endpoint in ("run", "login", "signup", "static"):
        return

    # Some requests might not have a resolvable endpoint
    if request.endpoint is None:
        return

    # If user is not in session, force them back to home
    if "user" not in session:
        return redirect(url_for("run"))


@app.route('/', methods=['GET', 'POST'])
def run():
    return render_template(
        "index.html",
        image_path='./static/images',
        js_path='static/js/script.js',
        css_path='static/css/style.css',
    )


def send_contact_email(name: str, from_email: str, message_body: str) -> None:
    """
    Send a simple email using SMTP to the configured CONTACT_RECIPIENT_EMAIL.
    Expects SMTP_* environment variables to be set for authentication.
    """
    if not SMTP_HOST or not SMTP_PORT or not CONTACT_RECIPIENT_EMAIL:
        # If email is not configured, silently skip sending to avoid hard crashes.
        return

    msg = EmailMessage()
    msg["Subject"] = f"New contact form message from {name}"
    msg["From"] = from_email or SMTP_USERNAME or CONTACT_RECIPIENT_EMAIL
    msg["To"] = CONTACT_RECIPIENT_EMAIL
    msg.set_content(
        f"New contact form submission:\n\n"
        f"From: {name} <{from_email}>\n\n"
        f"Message:\n{message_body}\n"
    )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            if SMTP_USERNAME and SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
    except Exception:
        # In production you might want to log this exception.
        pass


def password_is_strong(password: str) -> bool:
    """
    Ensure the password contains at least one uppercase, one lowercase,
    one digit, and one special character.
    """
    if len(password) < 8:
        return False
    has_upper = bool(re.search(r"[A-Z]", password))
    has_lower = bool(re.search(r"[a-z]", password))
    has_digit = bool(re.search(r"\d", password))
    has_special = bool(re.search(r"[^A-Za-z0-9]", password))
    return has_upper and has_lower and has_digit and has_special


def hash_user_password(email: str, phone: str, password: str) -> str:
    """
    Create a unique UUID-like salt based on email + phone, then hash the user's
    password combined with that salt. This satisfies:
      - unique UUID derived from email+phone
      - hashed user password stored in DB
    """
    base = (email or "").strip().lower() + (phone or "").strip()
    uuid_salt = uuid.uuid5(uuid.NAMESPACE_DNS, base or "anonymous")
    combined = (password or "") + uuid_salt.hex
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()

def normalize_word(w):
    """Very simple normalization to make plural/singular matches more forgiving."""
    w = w.lower()
    if len(w) > 3 and w.endswith("es"):
        return w[:-2]
    if len(w) > 3 and w.endswith("s"):
        return w[:-1]
    return w


def text_to_normalized_tokens(text):
    """Split text into words and normalize each one."""
    words = re.findall(r"\w+", (text or ""))
    return {normalize_word(w) for w in words if w}


def tokens_fuzzy_match(q_tokens, item_tokens, threshold=80):
    """
    Return True if any query token is a fuzzy match to any item token,
    using rapidfuzz with the given similarity threshold (0–100).
    """
    for q in q_tokens:
        for t in item_tokens:
            if fuzz.ratio(q, t) >= threshold:
                return True
    return False


@app.route('/inventory', methods=['GET'])
def get_inventory():
    query = request.args.get('q', '').strip()
    page = request.args.get('page', default=1, type=int)
    per_page = 15

    cassandra = Cassandra()
    data = cassandra.get_all_data('inventory')
    items = list(data)

    # Determine which items are currently in this user's cart
    cart_item_ids = set()
    cart_id = session.get('cart_id')
    if cart_id:
        cart_rows = cassandra.get_cart_items(cart_id)
        cart_item_ids = {str(row.item_id) for row in cart_rows}

    if query:
        q_tokens = text_to_normalized_tokens(query)
        filtered = []
        for item in items:
            name = getattr(item, "name", "") or ""
            desc = getattr(item, "description", "") or ""
            item_tokens = text_to_normalized_tokens(name + " " + desc)

            # Exact token match (after normalization)
            exact_hit = bool(q_tokens & item_tokens)
            # Fuzzy match on tokens to catch minor spelling/inflection differences
            fuzzy_hit = tokens_fuzzy_match(q_tokens, item_tokens, threshold=80)

            if exact_hit or fuzzy_hit:
                filtered.append(item)
        items = filtered

    total_items = len(items)
    if total_items == 0:
        total_pages = 1
    else:
        total_pages = (total_items + per_page - 1) // per_page

    # Clamp page to valid range
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start = (page - 1) * per_page
    end = start + per_page
    page_items = items[start:end]

    return render_template(
        "inventory.html",
        inventory=page_items,
        search_query=query,
        page=page,
        total_pages=total_pages,
        total_items=total_items,
        per_page=per_page,
        cart_item_ids=cart_item_ids,
    )


@app.route('/inventory/<item_id>', methods=['GET'])
def product_detail(item_id):
    cassandra = Cassandra()
    cart_id = session.get('cart_id')
    in_cart = False
    if cart_id:
        in_cart = cassandra.is_item_in_cart(cart_id, item_id)

    item = cassandra.get_item_by_id('inventory', item_id)
    if item is None:
        cassandra.shutdown()
        abort(404)
    # Load any additional images for this item; fall back to primary image_url
    images = cassandra.get_images_for_item(item_id)
    cassandra.shutdown()
    if not images:
        images = [item.image_url] if getattr(item, "image_url", None) else []

    # Format timestamps without milliseconds, including timezone (or assume UTC)
    def _format_ts(dt):
        if not dt:
            return ""
        # Ensure we drop microseconds
        dt = dt.replace(microsecond=0)
        tzinfo = dt.tzinfo
        if tzinfo is None or tzinfo.utcoffset(dt) is None:
            # Treat naive datetimes as UTC
            return dt.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

    created_display = _format_ts(getattr(item, "created_at", None))
    updated_display = _format_ts(getattr(item, "updated_at", None))

    return render_template(
        "product_detail.html",
        item=item,
        images=images,
        in_cart=in_cart,
        created_display=created_display,
        updated_display=updated_display,
    )


@app.route('/inventory/<item_id>/edit', methods=['GET', 'POST'])
def edit_product(item_id):
    cassandra = Cassandra()
    if request.method == 'POST':
        form = request.form
        update_data = {
            "name": form.get("name", "").strip(),
            "price": form.get("price", 0),
            "description": form.get("description", "").strip(),
            "image_url": form.get("image_url", "").strip(),
            "status": form.get("status", "available").strip().lower(),
        }
        # Build full image list: primary image_url plus any additional lines
        images_text = form.get("image_urls", "")
        extra_images = [
            line.strip()
            for line in images_text.splitlines()
            if line.strip()
        ]
        all_images = []
        primary_url = update_data["image_url"]
        if primary_url:
            all_images.append(primary_url)
        for url in extra_images:
            if url not in all_images:
                all_images.append(url)

        cassandra.update_item('inventory', item_id, update_data)
        if all_images:
            cassandra.set_images_for_item(item_id, all_images)
        cassandra.shutdown()
        return redirect(url_for('product_detail', item_id=item_id))
    else:
        item = cassandra.get_item_by_id('inventory', item_id)
        if item is None:
            cassandra.shutdown()
            abort(404)
        # Pre-populate image URLs: any stored images or fall back to the primary
        images = cassandra.get_images_for_item(item_id)
        cassandra.shutdown()
        if images:
            image_urls_text = "\n".join(images)
        else:
            image_urls_text = getattr(item, "image_url", "") or ""
        return render_template("edit_product.html", item=item, image_urls=image_urls_text)


@app.route('/contact', methods=['GET', 'POST'])
def contact():
    """
    Simple contact page with a form that sends an email to the configured recipient.
    """
    error = None

    if request.method == 'POST':
        name = (request.form.get("name") or "").strip()
        email_addr = (request.form.get("email") or "").strip()
        message = (request.form.get("message") or "").strip()

        if not name or not email_addr or not message:
            error = "Please fill in your name, email, and a message."
        else:
            try:
                send_contact_email(name, email_addr, message)
                # On successful send, redirect to a confirmation page
                return redirect(url_for('contact_sent'))
            except Exception:
                # If sending fails, show a friendly error
                error = "We were unable to send your message. Please try again later."

    return render_template(
        "contact.html",
        error=error,
    )


@app.route('/contact/sent', methods=['GET'])
def contact_sent():
    """
    Confirmation page shown after a contact email has been sent.
    Provides alternate contact options such as phone.
    """
    return render_template(
        "contact_sent.html",
        contact_email=CONTACT_RECIPIENT_EMAIL,
        contact_phone=CONTACT_PHONE,
    )


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    """
    Sign up page with an option to continue as guest.
    On sign up, create a Users record with usertype='customer'.
    """
    error = None

    if request.method == 'POST':
        action = request.form.get("action", "signup")

        if action == "guest":
            # Simple guest "sign-in": mark session and redirect
            session['user'] = {
                "type": "guest",
            }
            return redirect(url_for('get_inventory'))

        # Handle full sign-up
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        phone = (request.form.get("phone") or "").strip()
        password = (request.form.get("password") or "")
        confirm = (request.form.get("confirm_password") or "")

        if not first_name or not last_name or not email or not password or not confirm:
            error = "Please fill in first name, last name, email, and password fields."
        elif password != confirm:
            error = "Passwords do not match."
        elif not password_is_strong(password):
            error = "Password must be at least 8 characters and include uppercase, lowercase, numbers, and special characters."
        else:
            cassandra = Cassandra()
            cassandra.create_users_table()
            existing = cassandra.get_user_by_email(email)
            if existing:
                error = "An account with that email already exists."
                cassandra.shutdown()
            else:
                password_hash = hash_user_password(email, phone, password)
                user_id = cassandra.insert_user(
                    firstname=first_name,
                    lastname=last_name,
                    email=email,
                    password_hash=password_hash,
                    phone=phone or None,
                    usertype="customer",
                )
                cassandra.shutdown()

                # After successful sign up, send the user to the login page
                return redirect(url_for('login'))

    return render_template(
        "signup.html",
        error=error,
    )


@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Basic login page: asks for email and password, validates against the users table,
    and on success redirects to the main home page.
    """
    # Treat GET as a request to show the modal, not a standalone page
    if request.method == 'GET':
        # If the current user is a guest, clear that so the modal will appear
        user = session.get("user") or {}
        if user.get("type") == "guest":
            session.pop("user", None)
        next_url = request.args.get("next") or url_for('run')
        return redirect(next_url)

    # POST: handle login or guest actions, always redirect back
    error = None
    action = request.form.get("action", "login")
    next_url = request.form.get("next") or request.args.get("next") or url_for('run')

    # Allow users to continue as a guest
    if action == "guest":
        session['user'] = {"type": "guest"}
        return redirect(next_url)

    # Normal login flow
    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "")

    if not email or not password:
        error = "Please enter both username (email) and password."
    else:
        cassandra = Cassandra()
        cassandra.create_users_table()
        user = cassandra.get_user_by_email(email)

        if not user:
            error = "Invalid username or password."
        else:
            # Recompute the hash using the stored phone and the entered password
            stored_phone = getattr(user, "phone", "") or ""
            stored_hash = getattr(user, "password", "") or ""
            candidate_hash = hash_user_password(email, stored_phone, password)
            if candidate_hash != stored_hash:
                error = "Invalid username or password."
            else:
                # Successful login
                session['user'] = {
                    "id": str(getattr(user, "id", "")),
                    "name": getattr(user, "firstname", "") or "",
                    "email": email,
                    "type": getattr(user, "usertype", "") or "customer",
                }
                # If there was a guest cart, normalize its items to remove TTL
                cart_id = session.get("cart_id")
                if cart_id:
                    try:
                        cassandra.normalize_cart_items(cart_id)
                    except Exception:
                        # Don't block login if normalization fails
                        pass
                cassandra.shutdown()
                return redirect(next_url)

        cassandra.shutdown()

    # If we reach here there was a validation error; surface it via session
    session['login_error'] = error
    return redirect(next_url)


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    """
    Log the current user out and return to the home page.
    """
    # If there is an active cart, clear it and discard the cart_id
    cart_id = session.get('cart_id')
    if cart_id:
        cassandra = Cassandra()
        try:
            cassandra.clear_cart(cart_id)
        finally:
            cassandra.shutdown()
        session.pop('cart_id', None)

    session.pop('user', None)
    return redirect(url_for('run'))


@app.route('/cart', methods=['GET'])
def view_cart():
    items = []
    total = 0.0
    cart_id = session.get('cart_id')
    if cart_id:
        cassandra = Cassandra()
        try:
            cart_rows = cassandra.get_cart_items(cart_id)
            for row in cart_rows:
                item = cassandra.get_item_by_id('inventory', str(row.item_id))
                if item is not None:
                    items.append(item)
                    try:
                        qty = row.quantity or 1
                        total += float(item.price) * qty
                    except Exception:
                        pass
        finally:
            cassandra.shutdown()
    return render_template("cart.html", items=items, total=total)


@app.route('/cart/add/<item_id>', methods=['POST'])
def add_to_cart(item_id):
    # Only allow adding items that are still available
    cassandra = Cassandra()
    try:
        item = cassandra.get_item_by_id('inventory', item_id)
        if item is None or getattr(item, "status", "").lower() != "available":
            # Silently redirect back if the item is no longer available
            return redirect(url_for('product_detail', item_id=item_id))

        cart_id = get_or_create_cart_id()
        # For furniture, just store quantity = 1; prevent duplicates
        if not cassandra.is_item_in_cart(cart_id, item_id):
            user = session.get("user") or {}
            is_guest = user.get("type") == "guest"
            ttl = 3600 if is_guest else None  # 1 hour TTL for guest carts
            cassandra.add_item_to_cart(cart_id, item_id, quantity=1, ttl_seconds=ttl)
            # As soon as an item enters any cart, mark it as pending in inventory
            cassandra.mark_items_sold('inventory', [item_id])
    finally:
        cassandra.shutdown()
    return redirect(url_for('product_detail', item_id=item_id))


@app.route('/cart/remove/<item_id>', methods=['POST'])
def remove_from_cart(item_id):
    cart_id = session.get('cart_id')
    if cart_id:
        cassandra = Cassandra()
        try:
            cassandra.remove_item_from_cart(cart_id, item_id)
            # If no other cart still contains this item, mark it available again
            if not cassandra.item_is_in_any_cart(item_id):
                cassandra.mark_items_available('inventory', [item_id])
        finally:
            cassandra.shutdown()
    return redirect(url_for('view_cart'))


@app.route('/checkout', methods=['GET'])
def checkout():
    """Show summary, payment options, and terms."""
    items = []
    total = 0.0
    cart_id = session.get('cart_id')
    if cart_id:
        cassandra = Cassandra()
        try:
            cart_rows = cassandra.get_cart_items(cart_id)
            for row in cart_rows:
                item = cassandra.get_item_by_id('inventory', str(row.item_id))
                if item is not None:
                    items.append(item)
                    try:
                        qty = row.quantity or 1
                        total += float(item.price) * qty
                    except Exception:
                        pass
        finally:
            cassandra.shutdown()

    return render_template(
        "checkout.html",
        items=items,
        total=total,
        paypal_client_id=PAYPAL_CLIENT_ID,
    )


@app.route('/checkout/complete', methods=['GET'])
def checkout_complete():
    """Simple page shown after successful PayPal checkout."""
    return render_template("checkout_complete.html")


def _get_paypal_access_token():
    """Obtain an OAuth access token from PayPal."""
    if not PAYPAL_CLIENT_ID or not PAYPAL_CLIENT_SECRET:
        abort(500, description="PayPal is not configured on the server.")

    auth = (PAYPAL_CLIENT_ID, PAYPAL_CLIENT_SECRET)
    headers = {"Accept": "application/json", "Accept-Language": "en_US"}
    data = {"grant_type": "client_credentials"}
    resp = requests.post(f"{PAYPAL_API_BASE}/v1/oauth2/token", headers=headers, data=data, auth=auth)
    if resp.status_code != 200:
        abort(502, description="Failed to obtain PayPal access token.")
    return resp.json().get("access_token")


@app.route('/api/paypal/create-order', methods=['POST'])
def paypal_create_order():
    """Create a PayPal order for the current cart."""
    items = []
    total = 0.0
    cart_id = session.get('cart_id')

    if cart_id:
        cassandra = Cassandra()
        try:
            cart_rows = cassandra.get_cart_items(cart_id)
            for row in cart_rows:
                item = cassandra.get_item_by_id('inventory', str(row.item_id))
                if item is not None:
                    items.append(item)
                    try:
                        qty = row.quantity or 1
                        total += float(item.price) * qty
                    except Exception:
                        pass
        finally:
            cassandra.shutdown()

    if not items or total <= 0:
        return jsonify({"error": "Cart is empty or total is invalid."}), 400

    access_token = _get_paypal_access_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    body = {
        "intent": "CAPTURE",
        "purchase_units": [
            {
                "amount": {
                    "currency_code": "USD",
                    "value": f"{total:.2f}",
                },
                "description": "Chloe Nomura Home furniture order",
            }
        ]
    }
    resp = requests.post(f"{PAYPAL_API_BASE}/v2/checkout/orders", json=body, headers=headers)
    if resp.status_code not in (200, 201):
        return jsonify({"error": "Failed to create PayPal order."}), 502
    data = resp.json()
    return jsonify({"id": data.get("id")})


@app.route('/api/paypal/capture-order', methods=['POST'])
def paypal_capture_order():
    """Capture an approved PayPal order and mark items as pending."""
    payload = request.get_json(silent=True) or {}
    order_id = payload.get("orderID")
    if not order_id:
        return jsonify({"error": "orderID is required"}), 400

    access_token = _get_paypal_access_token()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
    }
    resp = requests.post(f"{PAYPAL_API_BASE}/v2/checkout/orders/{order_id}/capture", headers=headers)
    if resp.status_code not in (200, 201):
        return jsonify({"error": "Failed to capture PayPal order."}), 502

    capture_data = resp.json()
    status = capture_data.get("status")

    # Only mark items pending if PayPal says the order is completed
    if status == "COMPLETED":
        cart_id = session.get('cart_id')
        if cart_id:
            cassandra = Cassandra()
            try:
                cart_rows = cassandra.get_cart_items(cart_id)
                item_ids = [str(row.item_id) for row in cart_rows]
                if item_ids:
                    cassandra.mark_items_sold('inventory', item_ids)
                cassandra.clear_cart(cart_id)
            finally:
                cassandra.shutdown()
            session.pop('cart_id', None)

    return jsonify(
        {
            "status": status,
            "redirect_url": url_for('checkout_complete'),
        }
    )

if __name__ == '__main__':
    app.run(debug=True, port=5001)
