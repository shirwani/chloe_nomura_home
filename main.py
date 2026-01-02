from flask import Flask, render_template, request, url_for, jsonify, abort, redirect, session, g
from database import DBInterface, parse_roles
from inventory_search import InventorySearch
from types import SimpleNamespace
from datetime import datetime, timedelta
import hashlib
import os
import re
import smtplib
from email.message import EmailMessage
import uuid
import requests
from rapidfuzz import fuzz
import math

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


def get_db() -> DBInterface:
    """
    Return a per-request DBInterface instance backed by the MySQL connection
    pool. This avoids repeatedly opening new TCP/SSL connections for each
    spot in the code that needs database access.
    """
    if "db" not in g:
        g.db = DBInterface()
    return g.db


@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.shutdown()


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
        db = get_db()
        cart_count = db.get_cart_item_count(cart_id)

    user_first_name = user.get("name")
    roles = user.get("roles") or []
    is_guest = "guest" in roles
    is_admin = "admin" in roles

    login_error = session.pop("login_error", None)
    login_forgot_mode = session.pop("login_forgot_mode", False)
    force_login_overlay = session.pop("force_login_overlay", False)

    return {
        "cart_count": cart_count,
        "user_first_name": user_first_name,
        "user_roles": roles,
        "user_is_admin": is_admin,
        "user_is_guest": is_guest,
        "login_error": login_error,
        "login_forgot_mode": login_forgot_mode,
        "force_login_overlay": force_login_overlay,
    }


@app.before_request
def redirect_anonymous_to_home():
    """
    If the user is not logged in (and has not chosen guest),
    and they try to access any page other than the home page,
    send them to the home page, where the login modal will appear.
    """
    # Allow these endpoints without redirect
    if request.endpoint in (
        "home",
        "about",
        "login",
        "signup",
        "forgot_password",
        "forgot_password_send_link",
        "reset_password",
        "static",
    ):
        return

    # Some requests might not have a resolvable endpoint
    if request.endpoint is None:
        return

    # If user is not in session, force them back to home
    if "user" not in session:
        return redirect(url_for("home"))

@app.route('/', methods=['GET', 'POST'])
def run():
    return redirect(url_for("home"))    

@app.route('/home', methods=['GET', 'POST'])
def home():
    db = get_db()
    top_picks = []
    category_spotlights = []
    try:
        # Build a small "Top Picks" set for the home page:
        #  - status: available
        #  - not on discount (no original_price > price)
        #  - lowest view counts first (surface under-discovered items)
        all_items = list(db.get_all_data("inventory"))

        def _is_top_pick_candidate(it):
            status = (getattr(it, "status", "") or "").lower()
            if status != "available":
                return False

            price = getattr(it, "price", None)
            original_price = getattr(it, "original_price", None)
            try:
                price = float(price)
            except (TypeError, ValueError):
                return False

            try:
                original_price = float(original_price)
            except (TypeError, ValueError):
                original_price = None

            # Exclude discounted items (where original_price is a valid number and > price)
            if original_price is not None and original_price > price:
                return False

            return True

        candidates = [it for it in all_items if _is_top_pick_candidate(it)]

        def _view_count(it):
            v = getattr(it, "views", 0) or 0
            try:
                return int(v)
            except (TypeError, ValueError):
                return 0

        # Sort by view count ascending so low-view items show first
        candidates.sort(key=_view_count)
        top_picks = candidates[:10]

        # Build up to four "shop by category" spotlights using the same inventory
        # data we already loaded:
        #   - group all listed items by category
        #   - sort categories by descending item count
        #   - for each of the top categories, choose a single representative item
        #     (prefer one with an image_url when available)
        category_buckets: dict[str, list] = {}
        for it in all_items:
            cat = (getattr(it, "category", None) or "Other").strip()
            if not cat:
                cat = "Other"
            category_buckets.setdefault(cat, []).append(it)

        # Sort categories by how many items they contain (most items first)
        sorted_categories = sorted(
            category_buckets.items(),
            key=lambda kv: len(kv[1]),
            reverse=True,
        )

        category_spotlights = []
        for cat, items_in_cat in sorted_categories:
            # Skip the generic "Other" bucket; only show concrete categories.
            if cat == "Other" or not items_in_cat:
                continue
            # Prefer an item with a non-empty image_url, otherwise fall back to the first.
            with_image = [
                it for it in items_in_cat
                if getattr(it, "image_url", None)
            ]
            representative = with_image[0] if with_image else items_in_cat[0]
            category_spotlights.append(
                {
                    "category": cat,
                    "item": representative,
                }
            )
            if len(category_spotlights) >= 4:
                break
    except Exception:
        # If anything goes wrong loading top picks, fail silently and show none.
        top_picks = []
        category_spotlights = []

    return render_template(
        "index.html",
        image_path='./static/images',
        js_path='static/js/script.js',
        css_path='static/css/style.css',
        top_picks=top_picks,
        category_spotlights=category_spotlights,
    )


@app.route('/about', methods=['GET'])
def about():
    return render_template(
        "about.html",
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


def send_password_reset_email(to_email: str, reset_url: str) -> None:
    """
    Send a reset-password email with a link to the Reset Password page
    where the user can choose a new password.
    """
    if not SMTP_HOST or not SMTP_PORT or not to_email:
        return

    msg = EmailMessage()
    msg["Subject"] = "Chloe Nomura Home - Reset Your Password"
    msg["From"] = SMTP_USERNAME or CONTACT_RECIPIENT_EMAIL
    msg["To"] = to_email
    msg.set_content(
        "We received a request to reset your password for Chloe Nomura Home.\n\n"
        "To choose a new password, click the link below (or paste it into your browser).\n"
        "For your security, this link is only valid for 5 minutes:\n\n"
        f"{reset_url}\n\n"
        "If you did not request this, you can ignore this email."
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


def _prepare_categories_for_select(raw_categories):
    """
    Normalize and order category names for selection controls.
    All available categories are included, with 'Other' forced to the end.
    """
    if not raw_categories:
        # Fallback defaults with Other at the end
        base = ["Bedroom", "Living Room", "Study"]
        result = []
        for c in base:
            if c not in result:
                result.append(c)
        result.append("Other")
        return result

    cleaned = []
    for c in raw_categories:
        label = (c or "").strip()
        if label and label not in cleaned:
            cleaned.append(label)

    non_other = sorted([c for c in cleaned if c != "Other"])
    # Always include 'Other' at the bottom exactly once
    non_other.append("Other")
    return non_other


@app.route('/inventory', methods=['GET'])
def get_inventory():
    query = request.args.get('q', '').strip()
    page = request.args.get('page', default=1, type=int)
    # Show up to 20 items per page in the inventory.
    per_page = 20

    # Selected categories from the filter menu (can be multiple)
    selected_categories = [c for c in request.args.getlist("category") if c]
    # Selected statuses from the filter menu (can be multiple)
    selected_statuses = [s for s in request.args.getlist("status") if s]

    # Optional price filters (current sale price)
    min_price_raw = (request.args.get("min_price") or "").strip()
    max_price_raw = (request.args.get("max_price") or "").strip()
    min_price = None
    max_price = None
    try:
        if min_price_raw:
            min_price = float(min_price_raw)
    except ValueError:
        min_price = None
    try:
        if max_price_raw:
            max_price = float(max_price_raw)
    except ValueError:
        max_price = None

    # Special filters
    discount_only = (request.args.get("discount_only") == "1")
    hot_only = (request.args.get("hot_only") == "1")
    trending_only = (request.args.get("trending_only") == "1")

    db = get_db()

    # Determine which items are currently in this user's cart
    cart_item_ids = set()
    cart_id = session.get('cart_id')
    if cart_id:
        cart_rows = db.get_cart_items(cart_id)
        cart_item_ids = {str(row.item_id) for row in cart_rows}

    if query:
        # ----------------------------------------------
        # Search path: keep existing Python-level logic
        # ----------------------------------------------
        items = []
        # Use enhanced keyword + semantic search over inventory
        searcher = InventorySearch(db=db)
        search_results = searcher.search(query, top_k=None)
        items = [r["item"] for r in search_results]

        # Never show items that are marked as unlisted in the main inventory view.
        def _item_is_listed(it):
            status = (getattr(it, "status", "") or "").lower()
            return status != "unlisted"

        items = [it for it in items if _item_is_listed(it)]

        # Apply category filter if any categories were selected
        if selected_categories:
            normalized_selected = set(selected_categories)

            def item_in_selected_category(it):
                cat = getattr(it, "category", None) or "Other"
                return cat in normalized_selected

            items = [it for it in items if item_in_selected_category(it)]

        # Apply status filter if any statuses were selected
        if selected_statuses:
            normalized_statuses = {s.lower() for s in selected_statuses}

            def item_in_selected_status(it):
                status = (getattr(it, "status", "") or "").lower()
                return status in normalized_statuses

            items = [it for it in items if item_in_selected_status(it)]

        # Apply price filters if provided
        if min_price is not None:

            def item_meets_min_price(it):
                price = getattr(it, "price", None)
                try:
                    price = float(price)
                except (TypeError, ValueError):
                    return False
                return price >= min_price

            items = [it for it in items if item_meets_min_price(it)]

        if max_price is not None:

            def item_meets_max_price(it):
                price = getattr(it, "price", None)
                try:
                    price = float(price)
                except (TypeError, ValueError):
                    return False
                return price <= max_price

            items = [it for it in items if item_meets_max_price(it)]

        # Apply discount-only filter (items where original_price > price)
        if discount_only:

            def item_is_discounted(it):
                price = getattr(it, "price", None)
                original_price = getattr(it, "original_price", None)
                try:
                    price = float(price)
                    original_price = float(original_price)
                except (TypeError, ValueError):
                    return False
                return original_price > price

            items = [it for it in items if item_is_discounted(it)]

        # Apply hot-only filter (same popularity definition as template HOT badge)
        if hot_only:

            def item_is_hot(it):
                views = getattr(it, "views", 0) or 0
                likes = getattr(it, "likes", 0) or 0
                try:
                    views = float(views)
                    likes = float(likes)
                except (TypeError, ValueError):
                    return False
                if views <= 0:
                    return False
                popularity = math.ceil((likes * 100.0) / views)
                return popularity > 10

            items = [it for it in items if item_is_hot(it)]

        # Apply trending-only filter: top items by view count (exclude items with 0 views)
        if trending_only:

            def item_has_views(it):
                views = getattr(it, "views", 0) or 0
                try:
                    views = int(views)
                except (TypeError, ValueError):
                    return False
                return views > 0

            # Keep only items that have at least one view
            items = [it for it in items if item_has_views(it)]
            # Sort by views descending and keep at most 10
            items.sort(key=lambda it: getattr(it, "views", 0) or 0, reverse=True)
            if len(items) > 10:
                items = items[:10]

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

    else:
        # ----------------------------------------------
        # Non-search path: push filters and pagination into SQL
        # ----------------------------------------------
        where_clauses = ["LOWER(status) <> %s"]
        params = ["unlisted"]

        # Category filter
        if selected_categories:
            placeholders = ", ".join(["%s"] * len(selected_categories))
            where_clauses.append(f"category IN ({placeholders})")
            params.extend(selected_categories)

        # Status filter
        if selected_statuses:
            normalized_statuses = [s.lower() for s in selected_statuses]
            placeholders = ", ".join(["%s"] * len(normalized_statuses))
            where_clauses.append(f"LOWER(status) IN ({placeholders})")
            params.extend(normalized_statuses)

        # Price filters
        if min_price is not None:
            where_clauses.append("price >= %s")
            params.append(min_price)
        if max_price is not None:
            where_clauses.append("price <= %s")
            params.append(max_price)

        # Discount-only: original_price > price
        if discount_only:
            where_clauses.append(
                "original_price IS NOT NULL AND original_price > price"
            )

        # Hot-only: same definition as template HOT badge: popularity > 10
        if hot_only:
            where_clauses.append(
                "views > 0 AND (likes * 100.0 / views) > 10"
            )

        # Trending-only: items with views > 0, ordered by views desc, limited to 10
        if trending_only:
            where_clauses.append("views > 0")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Count matching rows
        count_sql = f"SELECT COUNT(*) AS cnt FROM inventory WHERE {where_sql}"
        count_cur = db._execute(count_sql, params)
        count_row = count_cur.fetchone()
        full_count = int(getattr(count_row, "cnt", 0) or 0)

        if trending_only:
            effective_total_items = min(full_count, 10)
        else:
            effective_total_items = full_count

        total_items = effective_total_items or 0

        if total_items == 0:
            total_pages = 1
            page = 1
            page_items = []
        else:
            total_pages = (total_items + per_page - 1) // per_page

            # Clamp page to valid range
            if page < 1:
                page = 1
            if page > total_pages:
                page = total_pages

            if trending_only:
                limit = min(per_page, 10)
                offset = (page - 1) * per_page
                order_by = "ORDER BY views DESC"
            else:
                limit = per_page
                offset = (page - 1) * per_page
                # Prefer newest items first when browsing inventory
                order_by = "ORDER BY created_at DESC"

            data_sql = f"""
                SELECT *
                FROM inventory
                WHERE {where_sql}
                {order_by}
                LIMIT %s OFFSET %s;
            """
            data_params = params + [limit, offset]
            data_cur = db._execute(data_sql, data_params)
            page_items = data_cur.fetchall()

    # Build a mapping of item_id -> list of image URLs for use in
    # the inventory thumbnail carousel. Always include the primary
    # image_url as the first entry when available.
    # Batch-load images for all items on this page to avoid N+1 queries
    item_ids_for_page = [
        str(getattr(it, "id", ""))
        for it in page_items
        if getattr(it, "id", None)
    ]
    images_by_item = db.get_images_for_items(item_ids_for_page)

    inventory_images = {}
    for it in page_items:
        item_id_str = str(getattr(it, "id", ""))
        if not item_id_str:
            continue
        imgs = list(images_by_item.get(item_id_str, []))
        primary_url = getattr(it, "image_url", None)
        if primary_url and primary_url not in imgs:
            imgs.insert(0, primary_url)
        inventory_images[item_id_str] = imgs

    # Build the dynamic category list for the inventory filter sidebar
    raw_categories = db.get_all_categories()
    categories_for_filter = _prepare_categories_for_select(raw_categories)

    return render_template(
        "inventory.html",
        inventory=page_items,
        search_query=query,
        page=page,
        total_pages=total_pages,
        total_items=total_items,
        per_page=per_page,
        cart_item_ids=cart_item_ids,
        selected_categories=selected_categories,
        selected_statuses=selected_statuses,
        min_price=min_price_raw,
        max_price=max_price_raw,
        discount_only=discount_only,
        hot_only=hot_only,
        trending_only=trending_only,
        inventory_images=inventory_images,
        categories_for_filter=categories_for_filter,
    )


@app.route('/inventory/<item_id>', methods=['GET'])
def product_detail(item_id):
    db = get_db()
    cart_id = session.get('cart_id')
    in_cart = False
    cart_item_ids = set()
    if cart_id:
        try:
            cart_rows = db.get_cart_items(cart_id)
            cart_item_ids = {str(row.item_id) for row in cart_rows}
            in_cart = str(item_id) in cart_item_ids
        except Exception:
            # If cart lookup fails, just treat as not in cart
            cart_item_ids = set()
            in_cart = False

    item = db.get_item_by_id('inventory', item_id)
    if item is None:
        abort(404)

    # Bump the view count each time the detail page is requested
    try:
        db.increment_item_view_count('inventory', item_id)
        # Re-fetch the item so the updated view count is reflected in the UI
        item = db.get_item_by_id('inventory', item_id)
    except Exception:
        # Do not block the page load if view-count tracking fails
        pass
    # Load any additional images for this item; fall back to primary image_url
    images = db.get_images_for_item(item_id)
    # Find up to 10 other items in the same category (if any), excluding this item
    related_items = []
    category = getattr(item, "category", None)
    if category:
        all_items = db.get_all_data('inventory')
        for other in all_items:
            if str(getattr(other, "id", "")) == str(item.id):
                continue
            if getattr(other, "category", None) == category:
                related_items.append(other)
            if len(related_items) >= 10:
                break
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
        related_items=related_items,
        cart_item_ids=cart_item_ids,
    )


@app.route('/inventory/<item_id>/like', methods=['POST'])
def like_item(item_id):
    """
    Toggle the like status for the current (non-guest) user on an inventory item.
    If the user has not liked the item yet, this will add a like; otherwise it
    will remove their like. The aggregate like count is kept in the inventory
    table, and individual user likes are tracked in a separate table.
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []
    user_id = user.get("id")

    # Only logged-in, non-guest users can like items
    if not user_id or "guest" in roles:
        # For AJAX callers, surface an error; for normal form posts, just redirect back.
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"error": "login_required"}), 403
        ref = request.referrer or url_for('product_detail', item_id=item_id)
        return redirect(ref)

    db = get_db()
    item = db.get_item_by_id('inventory', item_id)
    if item is None:
        abort(404)
    # Determine whether this user has already liked the item
    already_liked = db.user_has_liked_item(str(user_id), item_id)
    if already_liked:
        db.remove_like_for_item(str(user_id), item_id, 'inventory')
    else:
        db.add_like_for_item(str(user_id), item_id, 'inventory')
    # Re-fetch the item so we can return the updated like count
    item = db.get_item_by_id('inventory', item_id)
    new_likes = getattr(item, "likes", 0) or 0
    views = getattr(item, "views", 0) or 0
    if views > 0:
        popularity = math.ceil((new_likes * 100.0) / views)
    else:
        popularity = 0
    # If the client expects JSON (e.g., an AJAX call), return the new count
    if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify(
            {
                "item_id": item_id,
                "likes": new_likes,
                "popularity": popularity,
            }
        )

    # Default behavior: redirect back to the page the user came from
    ref = request.referrer
    if ref:
        return redirect(ref)
    return redirect(url_for('product_detail', item_id=item_id))


@app.route('/inventory/<item_id>/edit', methods=['GET', 'POST'])
def edit_product(item_id):
    db = get_db()
    if request.method == 'POST':
        form = request.form
        category = (form.get("category") or "").strip()
        price_raw = (form.get("price") or "0").strip()
        try:
            price = float(price_raw or 0)
        except ValueError:
            price = 0.0

        original_price_raw = (form.get("original_price") or "").strip()

        update_data = {
            "name": form.get("name", "").strip(),
            "price": price,
            "original_price": original_price_raw,
            "category": category,
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

        db.update_item('inventory', item_id, update_data)
        if all_images:
            db.set_images_for_item(item_id, all_images)
        return redirect(url_for('product_detail', item_id=item_id))
    else:
        item = db.get_item_by_id('inventory', item_id)
        if item is None:
            abort(404)
        # Pre-populate image URLs: any stored images or fall back to the primary
        images = db.get_images_for_item(item_id)
        raw_categories = db.get_all_categories()
        if images:
            image_urls_text = "\n".join(images)
        else:
            image_urls_text = getattr(item, "image_url", "") or ""
        categories_for_select = _prepare_categories_for_select(raw_categories)
        return render_template(
            "edit_product.html",
            item=item,
            image_urls=image_urls_text,
            is_new=False,
            categories_for_select=categories_for_select,
        )


@app.route('/inventory/<item_id>/delete', methods=['POST'])
def delete_product(item_id):
    """
    Admin-only: permanently delete an inventory item and its related records.
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []
    if "admin" not in roles:
        abort(403)

    db = get_db()
    item = db.get_item_by_id('inventory', item_id)
    if item is None:
        abort(404)
    db.delete_inventory_item(item_id)
    return redirect(url_for('get_inventory'))


@app.route('/inventory/add', methods=['GET', 'POST'])
def add_product():
    """
    Admin-only: add a new inventory item.
    Reuses the edit_product template with an empty item.
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []
    if "admin" not in roles:
        abort(403)

    db = get_db()
    if request.method == "POST":
        form = request.form
        name = (form.get("name") or "").strip()
        price_raw = (form.get("price") or "0").strip()
        original_price_raw = (form.get("original_price") or "").strip()
        description = (form.get("description") or "").strip()
        image_url = (form.get("image_url") or "").strip()
        status = (form.get("status") or "available").strip().lower() or "available"
        category = (form.get("category") or "").strip()

        try:
            price = float(price_raw or 0)
        except ValueError:
            price = 0.0

        insert_data = {
            "name": name,
            "price": price,
            "original_price": original_price_raw,
            "description": description,
            "image_url": image_url,
            "status": status,
            "category": category,
        }

        # Build image list (primary + extra)
        images_text = form.get("image_urls", "")
        extra_images = [
            line.strip()
            for line in images_text.splitlines()
            if line.strip()
        ]
        all_images = []
        if image_url:
            all_images.append(image_url)
        for url in extra_images:
            if url not in all_images:
                all_images.append(url)

        # Insert new item
        item_id = db.insert_data("inventory", insert_data)
        if all_images:
            db.set_images_for_item(item_id, all_images)
        return redirect(url_for("product_detail", item_id=item_id))

    # GET: render an empty item for the form
    # Include all attributes referenced in the template so Jinja
    # does not raise UndefinedError when accessing them.
    raw_categories = db.get_all_categories()
    categories_for_select = _prepare_categories_for_select(raw_categories)
    empty_item = SimpleNamespace(
        id="",
        name="",
        price=0.0,
        original_price=None,
        description="",
        image_url="",
        status="available",
        category="Bedroom",
    )
    return render_template(
        "edit_product.html",
        item=empty_item,
        image_urls="",
        is_new=True,
        categories_for_select=categories_for_select,
    )


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

    # If the user is logged in, pre-fill their name and email in the form by default.
    # The template will prefer any values the user just submitted (request.form),
    # but fall back to these when the form is first loaded.
    session_user = session.get("user") or {}
    contact_name = session_user.get("name", "") or ""
    contact_email = session_user.get("email", "") or ""

    return render_template(
        "contact.html",
        error=error,
        contact_name=contact_name,
        contact_email=contact_email,
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


@app.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    """
    Allow a user to choose a new password by providing their email address
    and a new password. On success, redirect back to the home page where the
    login dialog will appear.
    """
    error = None

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if not email:
            error = "Please enter your email address."
        elif not password or not confirm:
            error = "Please enter and confirm your new password."
        elif password != confirm:
            error = "Passwords do not match."
        elif not password_is_strong(password):
            error = "Password must be at least 8 characters and include uppercase, lowercase, numbers, and special characters."
        else:
            db = get_db()
            user = db.get_user_by_email(email)
            if not user:
                error = "We could not find an account with that email address."
            else:
                phone = getattr(user, "phone", "") or ""
                new_hash = hash_user_password(email, phone, password)
                db.update_user(str(user.id), {"password": new_hash})
                # After successful reset, send user back to home with login dialog
                return redirect(url_for("home"))
    return render_template(
        "forgot_password.html",
        error=error,
    )


@app.route('/forgot-password/send-link', methods=['POST'])
def forgot_password_send_link():
    """
    Handle the inline \"Forgot password\" flow from the login modal:
    send an email with a link back to the Forgot Password page.
    """
    # Prefer the inline forgot-password email field, but fall back to the
    # generic "email" field name for safety.
    email = (
        request.form.get("reset_email")
        or request.form.get("email")
        or ""
    ).strip().lower()
    if email:
        db = get_db()
        user = db.get_user_by_email(email)
        if user:
            # Create a one-time token that is valid for 5 minutes.
            token = uuid.uuid4().hex
            expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
            db.create_password_reset_token(str(user.id), token, expires_at)

            reset_url = url_for("reset_password", token=token, _external=True)
            send_password_reset_email(email, reset_url)
        session['login_error'] = (
            "If an account with that email exists, we've emailed a reset link. "
            "Please check your email for the link to reset your password."
        )
        session['login_forgot_mode'] = True

    # Redirect back to home; the login modal will show with the message.
    return redirect(url_for("home"))


@app.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    """
    Handle password reset using a one-time token from the email link.
    On success, redirect back to the home page, where the login dialog will appear.
    """
    db = get_db()
    error = None
    row = db.get_password_reset_token(token)
    if not row:
        error = "This password reset link is invalid or has expired."
    else:
        # Validate expiration
        try:
            expires_at = datetime.fromisoformat(row.expires_at)
        except Exception:
            expires_at = None

        if not expires_at or datetime.utcnow() > expires_at:
            db.delete_password_reset_token(token)
            error = "This password reset link has expired. Please request a new one."
        elif request.method == "POST":
            password = request.form.get("password") or ""
            confirm = request.form.get("confirm_password") or ""

            if not password or not confirm:
                error = "Please enter and confirm your new password."
            elif password != confirm:
                error = "Passwords do not match."
            elif not password_is_strong(password):
                error = "Password must be at least 8 characters and include uppercase, lowercase, numbers, and special characters."
            else:
                user = db.get_user_by_id(str(row.user_id))
                if not user:
                    error = "User account could not be found."
                else:
                    email = (getattr(user, "email", "") or "").lower()
                    phone = getattr(user, "phone", "") or ""
                    new_hash = hash_user_password(email, phone, password)
                    db.update_user(str(user.id), {"password": new_hash})
                    db.delete_password_reset_token(token)
                    # After successful reset, send user back to home with login dialog
                    return redirect(url_for("home"))
    return render_template(
        "reset_password.html",
        error=error,
    )


@app.route('/admin/users', methods=['GET', 'POST'])
def admin_users():
    """
    Admin-only user management page.
    Allows searching existing users and updating basic user info (not passwords).
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []
    if "admin" not in roles:
        abort(403)

    db = get_db()
    error = None
    success = None

    # If this is a POST (row update), process the update first.
    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "update":
            user_id = (request.form.get("user_id") or "").strip()
            if not user_id:
                error = "Missing user id for update."
            else:
                existing = db.get_user_by_id(user_id)
                if not existing:
                    error = "User not found."
                else:
                    first_name = (request.form.get("first_name") or "").strip()
                    last_name = (request.form.get("last_name") or "").strip()
                    phone = (request.form.get("phone") or "").strip()
                    usertype_raw = (request.form.get("usertype") or "").strip().lower()

                    update_data = {}
                    if first_name:
                        update_data["firstname"] = first_name
                    if last_name:
                        update_data["lastname"] = last_name
                    if phone:
                        update_data["phone"] = phone
                    if usertype_raw:
                        update_data["usertype"] = usertype_raw

                    if not error and update_data:
                        db.update_user(user_id, update_data)
                        # Remember which user row was just updated so the
                        # template can highlight it briefly.
                        session["admin_updated_user_id"] = user_id

    # Determine search query. Prefer the hidden field from POST (so the
    # table stays visible after an update), but fall back to the GET
    # query-string parameter used by the search box.
    form_search_query = (request.form.get("search_query") or "").strip() if request.method == "POST" else ""
    get_search_query = (request.args.get("q") or "").strip()
    search_query = form_search_query or get_search_query

    # If there is a search term, show matching users; otherwise, show none.
    if search_query:
        users = db.search_users(search_query)
    else:
        users = []

    # Load distinct inventory categories for the admin categories section
    categories = db.get_all_categories()
    # Pop any one-time highlight user/category identifiers to drive a transient row highlight.
    updated_user_id = session.pop("admin_updated_user_id", None)
    updated_category_name = session.pop("admin_updated_category_name", None)

    return render_template(
        "admin.html",
        users=users,
        search_query=search_query,
        error=error,
        success=success,
        updated_user_id=updated_user_id,
        updated_category_name=updated_category_name,
        categories=categories,
    )


@app.route('/admin/categories', methods=['POST'])
def admin_categories():
    """
    Admin-only endpoint for simple category management:
      - rename an existing category
      - delete a category (reassigning items to \"Other\")
      - add a new category label
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []
    if "admin" not in roles:
        abort(403)

    action = (request.form.get("action") or "").strip()
    db = get_db()
    if action == "rename":
        old_name = (request.form.get("old_name") or "").strip()
        new_name = (request.form.get("new_name") or "").strip()
        if old_name and new_name and old_name != new_name:
            db.rename_category(old_name, new_name)
            # Remember which category name was just updated so the
            # template can briefly highlight it.
            session["admin_updated_category_name"] = new_name
    elif action == "delete":
        name = (request.form.get("name") or "").strip()
        if name:
            # Reassign any items that currently use this category to \"Other\"
            db.delete_category_and_reassign(name, fallback="Other")
    elif action == "add":
        name = (request.form.get("name") or "").strip()
        # Creating a brand new category in this app is accomplished by
        # assigning it to at least one inventory item. To make the name
        # appear immediately in the categories list, ensure there is at
        # least a placeholder row using it.
        if name:
            db.add_category_if_missing(name)
            session["admin_updated_category_name"] = name
    # Always return to the main admin page after a category change.
    return redirect(url_for("admin_users"))


@app.route('/profile', methods=['GET', 'POST'])
def profile():
    """
    Profile page for logged-in (non-guest) users to update their own basic info.
    """
    session_user = session.get("user") or {}
    roles = session_user.get("roles") or []
    # Guests and anonymous users should not access profile
    if not session_user or "guest" in roles:
        abort(403)

    user_id = session_user.get("id")
    if not user_id:
        abort(403)

    # Simple pagination for sales history
    page_size = 10
    page = request.args.get("page", default=1, type=int)
    if page < 1:
        page = 1

    db = get_db()
    error = None
    success = None
    recent_sales = []
    total_sales = 0
    total_pages = 1

    user = db.get_user_by_id(str(user_id))
    if not user:
        abort(404)

    # Load paginated sales for this user
    total_sales = db.count_sales_for_user(str(user_id))
    if total_sales > 0:
        total_pages = (total_sales + page_size - 1) // page_size
        if page > total_pages:
            page = total_pages
        offset = (page - 1) * page_size
        recent_sales = db.get_sales_for_user(str(user_id), limit=page_size, offset=offset)

    if request.method == "POST":
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        phone = (request.form.get("phone") or "").strip()

        if not first_name or not last_name:
            error = "First and last name are required."
        else:
            update_data = {
                "firstname": first_name,
                "lastname": last_name,
                "phone": phone or None,
            }
            db.update_user(str(user.id), update_data)
            success = "Your profile has been updated."
            # Refresh user from DB and keep session in sync
            user = db.get_user_by_id(str(user.id))
            session["user"]["name"] = getattr(user, "firstname", "") or ""
    # Include any one-time success message from password link flow
    flash_success = session.pop("profile_success", None)
    if flash_success and not success:
        success = flash_success

    return render_template(
        "profile.html",
        user=user,
        recent_sales=recent_sales,
        page=page,
        total_pages=total_pages,
        total_sales=total_sales,
        error=error,
        success=success,
    )


@app.route('/profile/send-password-link', methods=['POST'])
def profile_send_password_link():
    """
    Send the logged-in user an email with a link to change their password.
    """
    session_user = session.get("user") or {}
    roles = session_user.get("roles") or []
    if not session_user or "guest" in roles:
        abort(403)

    user_id = session_user.get("id")
    if not user_id:
        abort(403)

    db = get_db()
    user = db.get_user_by_id(str(user_id))
    if user:
        email = (getattr(user, "email", "") or "").strip().lower()
        if email:
            token = uuid.uuid4().hex
            expires_at = (datetime.utcnow() + timedelta(minutes=5)).isoformat()
            db.create_password_reset_token(str(user.id), token, expires_at)

            reset_url = url_for("reset_password", token=token, _external=True)
            send_password_reset_email(email, reset_url)
    session["profile_success"] = (
        "If an account with your email exists, we've emailed a link to change your password."
    )
    return redirect(url_for("profile"))


@app.route('/order_details/<order_id>', methods=['GET'])
def order_detail(order_id):
    """
    Detail view for a single sale/order. Only the owning user (or admins)
    may view a sale.
    """
    session_user = session.get("user") or {}
    roles = session_user.get("roles") or []
    if not session_user or "guest" in roles:
        abort(403)

    current_user_id = session_user.get("id")
    if not current_user_id and "admin" not in roles:
        abort(403)

    db = get_db()
    sale = db.get_sale_by_id(str(order_id))
    if not sale:
        abort(404)

    # Enforce that only the owner (or admin) can see this sale
    sale_user_id = getattr(sale, "user_id", None)
    if sale_user_id:
        if str(sale_user_id) != str(current_user_id) and "admin" not in roles:
            abort(403)
    else:
        # If the sale has no associated user, restrict to admins
        if "admin" not in roles:
            abort(403)

    items = db.get_items_for_sale(str(order_id))
    return render_template(
        "order_details.html",
        sale=sale,
        items=items,
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
                "roles": ["guest"],
            }
            # After choosing to continue as guest from the signup page,
            # send the user to the home page where they can browse.
            return redirect(url_for('home'))

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
            db = get_db()
            db.create_users_table()
            existing = db.get_user_by_email(email)
            if existing:
                error = "An account with that email already exists."
            else:
                password_hash = hash_user_password(email, phone, password)
                user_id = db.insert_user(
                    firstname=first_name,
                    lastname=last_name,
                    email=email,
                    password_hash=password_hash,
                    phone=phone or None,
                    usertype="customer",
                )
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
        roles = user.get("roles") or []
        if "guest" in roles:
            session.pop("user", None)
        next_url = request.args.get("next") or url_for('home')
        return redirect(next_url)

    # POST: handle login or guest actions, always redirect back
    error = None
    action = request.form.get("action", "login")
    next_url = request.form.get("next") or request.args.get("next") or url_for('home')

    # Allow users to continue as a guest
    if action == "guest":
        session['user'] = {"roles": ["guest"]}
        return redirect(next_url)

    # Normal login flow
    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "")

    if not email or not password:
        error = "Please enter both username (email) and password."
    else:
        db = get_db()
        db.create_users_table()
        user = db.get_user_by_email(email)

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
                roles = parse_roles(getattr(user, "usertype", None)) or ["customer"]
                session['user'] = {
                    "id": str(getattr(user, "id", "")),
                    "name": getattr(user, "firstname", "") or "",
                    "email": email,
                    "roles": roles,
                }
                # If there was a guest cart, normalize its items to remove TTL
                cart_id = session.get("cart_id")
                if cart_id:
                    try:
                        db.normalize_cart_items(cart_id)
                    except Exception:
                        # Don't block login if normalization fails
                        pass
                return redirect(next_url)

    # If we reach here there was a validation error; surface it via session
    session['login_error'] = error
    return redirect(next_url)


@app.route('/login/overlay', methods=['GET'])
def login_overlay():
    """
    For guest users, keep them on the current page but force the inline
    login dialog to appear instead of navigating to the standalone login
    route. For all other users, fall back to the normal login behavior.
    """
    user = session.get("user") or {}
    roles = user.get("roles") or []

    next_url = request.args.get("next") or request.referrer or url_for("home")

    # Only special-case guests; everyone else uses the normal login flow.
    if "guest" in roles:
        session["force_login_overlay"] = True
        return redirect(next_url)

    return redirect(url_for("login", next=next_url))


@app.route('/logout', methods=['GET', 'POST'])
def logout():
    """
    Log the current user out and return to the home page.
    """
    # If there is an active cart, clear it and discard the cart_id
    cart_id = session.get('cart_id')
    if cart_id:
        db = get_db()
        # Capture the items currently in this cart so we can potentially
        # release them back to "available" status after clearing.
        cart_rows = db.get_cart_items(cart_id)
        item_ids = {str(row.item_id) for row in cart_rows}

        # Clear all items from this cart
        db.clear_cart(cart_id)

        # For each item that is no longer present in any cart at all,
        # mark it available again (mirrors the explicit remove behavior).
        items_to_mark_available = []
        for item_id in item_ids:
            if not db.item_is_in_any_cart(item_id):
                items_to_mark_available.append(item_id)
        if items_to_mark_available:
            db.mark_items_available('inventory', items_to_mark_available)
        session.pop('cart_id', None)

    session.pop('user', None)
    return redirect(url_for('home'))


@app.route('/cart', methods=['GET'])
def view_cart():
    items = []
    total = 0.0
    cart_id = session.get('cart_id')
    if cart_id:
        db = get_db()
        cart_rows = db.get_cart_items(cart_id)
        for row in cart_rows:
            item = db.get_item_by_id('inventory', str(row.item_id))
            if item is not None:
                items.append(item)
                try:
                    qty = row.quantity or 1
                    total += float(item.price) * qty
                except Exception:
                    pass

    return render_template("cart.html", items=items, total=total)


@app.route('/cart/add/<item_id>', methods=['POST'])
def add_to_cart(item_id):
    # Guests are not allowed to add items to the cart; instead, show the login dialog.
    user = session.get("user") or {}
    roles = user.get("roles") or []
    if "guest" in roles:
        session["force_login_overlay"] = True
        ref = request.referrer
        if ref:
            return redirect(ref)
        return redirect(url_for("home"))

    # Only allow adding items that are still available
    db = get_db()
    item = db.get_item_by_id('inventory', item_id)
    if item is None or getattr(item, "status", "").lower() != "available":
        # Silently redirect back if the item is no longer available
        return redirect(url_for('product_detail', item_id=item_id))

    cart_id = get_or_create_cart_id()
    # For furniture, just store quantity = 1; prevent duplicates
    if not db.is_item_in_cart(cart_id, item_id):
        ttl = None
        db.add_item_to_cart(cart_id, item_id, quantity=1, ttl_seconds=ttl)
        # As soon as an item enters any cart, mark it as pending in inventory
        db.mark_items_pending('inventory', [item_id])
    return redirect(url_for('product_detail', item_id=item_id))


@app.route('/cart/remove/<item_id>', methods=['POST'])
def remove_from_cart(item_id):
    cart_id = session.get('cart_id')
    if cart_id:
        db = get_db()
        db.remove_item_from_cart(cart_id, item_id)
        # If no other cart still contains this item, mark it available again
        if not db.item_is_in_any_cart(item_id):
            db.mark_items_available('inventory', [item_id])
    return redirect(url_for('view_cart'))


def _generate_payment_confirmation(length: int = 16) -> str:
    """
    Generate a random alphanumeric confirmation code of the given length.
    """
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(uuid.uuid4().hex[i % len(uuid.uuid4().hex)].upper() if i < 32 else alphabet[i % len(alphabet)]
                   for i in range(length))


@app.route('/cart/generate-test-sale', methods=['POST'])
def generate_test_sale():
    """
    Tester-only endpoint that converts the current cart into a test sale
    and records entries in Sales, items_sold, and payments tables, then
    shows the payment confirmation page.
    """
    session_user = session.get("user") or {}
    roles = session_user.get("roles") or []
    if "tester" not in roles:
        abort(403)

    cart_id = session.get('cart_id')
    if not cart_id:
        return redirect(url_for('view_cart'))

    db = get_db()
    cart_rows = db.get_cart_items(cart_id)
    items = []
    for row in cart_rows:
        item = db.get_item_by_id('inventory', str(row.item_id))
        if item is not None:
            items.append(item)

    if not items:
        return redirect(url_for('view_cart'))

    # For test sales, assume zero shipping fee by default
    shipping_fee = 0.0
    confirmation = _generate_payment_confirmation(16)
    user_id = session_user.get("id")

    sale = db.create_sale(
        user_id=user_id,
        items=items,
        shipping_fee=shipping_fee,
        payment_method="PayPal",
        payment_confirmation_number=confirmation,
    )

    if sale:
        # Stash details for the confirmation page
        session["last_sale"] = {
            "sale_id": sale.sale_id,
            "payment_id": sale.payment_id,
            "total": float(sale.total),
        }

    # Clear cart after recording the test sale and free inventory if needed
    item_ids = [str(getattr(it, "id", "")) for it in items if getattr(it, "id", None)]
    db.clear_cart(cart_id)
    for item_id in item_ids:
        if not db.item_is_in_any_cart(item_id):
            db.mark_items_sold('inventory', [item_id])
    session.pop('cart_id', None)

    return redirect(url_for('checkout_complete'))


@app.route('/checkout', methods=['GET'])
def checkout():
    """Show summary, payment options, and terms."""
    items = []
    total = 0.0
    cart_id = session.get('cart_id')
    if cart_id:
        db = get_db()
        cart_rows = db.get_cart_items(cart_id)
        for row in cart_rows:
            item = db.get_item_by_id('inventory', str(row.item_id))
            if item is not None:
                items.append(item)
                try:
                    qty = row.quantity or 1
                    total += float(item.price) * qty
                except Exception:
                    pass

    # Determine if the current user is a tester for showing test-sale UI
    session_user = session.get("user") or {}
    roles = session_user.get("roles") or []
    is_tester = "tester" in roles

    return render_template(
        "checkout.html",
        items=items,
        total=total,
        paypal_client_id=PAYPAL_CLIENT_ID,
        is_tester=is_tester,
    )


@app.route('/checkout/complete', methods=['GET'])
def checkout_complete():
    """Simple page shown after successful PayPal checkout."""
    last_sale = session.pop("last_sale", None)
    return render_template("checkout_complete.html", last_sale=last_sale)


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
    cart_items = []
    paypal_items = []
    total = 0.0
    cart_id = session.get('cart_id')

    if cart_id:
        db = get_db()
        cart_rows = db.get_cart_items(cart_id)
        for row in cart_rows:
            item = db.get_item_by_id('inventory', str(row.item_id))
            if item is not None:
                cart_items.append(item)
                try:
                    qty = row.quantity or 1
                    price = float(getattr(item, "price", 0) or 0)
                    total += price * qty
                    paypal_items.append(
                        {
                            "name": getattr(item, "name", "") or f"Item {item.id}",
                            "sku": str(getattr(item, "id", "") or ""),
                            "unit_amount": {
                                "currency_code": "USD",
                                "value": f"{price:.2f}",
                            },
                            "quantity": str(qty),
                        }
                    )
                except Exception:
                    # If anything goes wrong computing a line item, skip that item
                    continue

    if not cart_items or total <= 0:
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
                # Expose individual line items so item IDs (sku) appear in PayPal
                "items": paypal_items,
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
            db = get_db()
            cart_rows = db.get_cart_items(cart_id)
            items = []
            item_ids = []
            for row in cart_rows:
                item = db.get_item_by_id('inventory', str(row.item_id))
                if item is not None:
                    items.append(item)
                    item_ids.append(str(row.item_id))

            # Record the sale in the database
            user = session.get("user") or {}
            user_id = user.get("id")
            shipping_fee = 0.0
            sale = None
            if items:
                sale = db.create_sale(
                    user_id=user_id,
                    items=items,
                    shipping_fee=shipping_fee,
                    payment_method="PayPal",
                    payment_confirmation_number=order_id,
                )

            if sale:
                # Stash details for the confirmation page
                session["last_sale"] = {
                    "sale_id": sale.sale_id,
                    "payment_id": sale.payment_id,
                    "total": float(sale.total),
                }

            if item_ids:
                db.mark_items_sold('inventory', item_ids)
            db.clear_cart(cart_id)
            session.pop('cart_id', None)

    return jsonify(
        {
            "status": status,
            "redirect_url": url_for('checkout_complete'),
        }
                           )

if __name__ == '__main__':
    app.run(debug=True, port=5007)
