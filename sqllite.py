import sqlite3
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4


DB_PATH = "chloe.db"


def parse_roles(raw: str | None) -> list[str]:
    """
    Convert a comma-separated roles string into a normalized list of roles.
    Example: "admin, cashier" -> ["admin", "cashier"]
    """
    if not raw:
        return []
    return [part.strip().lower() for part in str(raw).split(",") if part.strip()]


def format_roles(roles) -> str:
    """
    Convert a list of roles into a single comma-separated string for storage.
    """
    if roles is None:
        return ""
    if isinstance(roles, str):
        # assume it's already a suitable representation
        return roles
    # de-duplicate while preserving order
    seen = set()
    normalized = []
    for r in roles:
        r_norm = str(r).strip().lower()
        if r_norm and r_norm not in seen:
            seen.add(r_norm)
            normalized.append(r_norm)
    return ",".join(normalized)


class DBInterface:
    """
    Compatibility wrapper that exposes the same interface as the previous
    Cassandra-based implementation, but uses a local SQLite database instead.
    """

    def __init__(self, db_path: str = DB_PATH):
        # check_same_thread=False so we can reuse this connection across Flask requests
        self.conn = sqlite3.connect(
            db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            check_same_thread=False,
        )
        self.conn.row_factory = self._row_factory

        # Ensure core tables exist
        self.create_inventory_table()
        self._ensure_inventory_category_column()
        self._ensure_inventory_discount_columns()
        self._ensure_inventory_metrics_columns()
        self._ensure_inventory_likes_table()
        self.create_users_table()
        self._ensure_images_table()
        self._ensure_cart_items_table()
        self._ensure_password_reset_table()
        self._ensure_orders_tables()
        self.backfill_inventory_categories()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _row_factory(cursor, row):
        """
        Convert SQLite rows into simple objects with attribute access,
        and parse timestamp fields into datetime objects when possible.
        """
        data = {}
        for idx, col in enumerate(cursor.description):
            name = col[0]
            val = row[idx]
            if name in ("created_at", "updated_at") and isinstance(val, str):
                try:
                    val = datetime.fromisoformat(val)
                except Exception:
                    pass
            data[name] = val
        return SimpleNamespace(**data)

    def _execute(self, query: str, params=()):
        cur = self.conn.execute(query, params)
        self.conn.commit()
        return cur

    def shutdown(self):
        try:
            self.conn.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Schema helpers
    # ------------------------------------------------------------------

    def create_inventory_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS inventory (
                id          TEXT PRIMARY KEY,
                name        TEXT,
                price       REAL,
                original_price REAL,
                description TEXT,
                image_url   TEXT,
                created_at  TEXT,
                updated_at  TEXT,
                status      TEXT,
                category    TEXT,
                views       INTEGER DEFAULT 0,
                likes       INTEGER DEFAULT 0
            );
        """
        self._execute(query)

    def _ensure_inventory_category_column(self):
        """
        Ensure the inventory table has a 'category' TEXT column.
        """
        cur = self._execute("PRAGMA table_info(inventory);")
        columns = [row.name for row in cur.fetchall()]
        if "category" not in columns:
            # Add a nullable category column; we'll backfill values separately.
            self._execute("ALTER TABLE inventory ADD COLUMN category TEXT;")

    def _ensure_inventory_discount_columns(self):
        """
        Ensure the inventory table has an 'original_price' REAL column used
        to represent the non-discounted price when an item is on sale.

        The current selling price continues to live in the 'price' column.
        """
        cur = self._execute("PRAGMA table_info(inventory);")
        columns = [row.name for row in cur.fetchall()]
        if "original_price" not in columns:
            # Nullable so existing rows remain valid; when null or <= price,
            # the item is treated as not discounted.
            self._execute("ALTER TABLE inventory ADD COLUMN original_price REAL;")

    def _ensure_inventory_metrics_columns(self):
        """
        Ensure the inventory table has integer 'views' and 'likes' columns
        used for simple engagement tracking.

        Both columns default to 0 for new and existing rows.
        """
        cur = self._execute("PRAGMA table_info(inventory);")
        columns = [row.name for row in cur.fetchall()]
        if "views" not in columns:
            self._execute(
                "ALTER TABLE inventory ADD COLUMN views INTEGER DEFAULT 0;"
            )
        if "likes" not in columns:
            self._execute(
                "ALTER TABLE inventory ADD COLUMN likes INTEGER DEFAULT 0;"
            )

    def _ensure_inventory_likes_table(self):
        """
        Ensure the inventory_likes table exists.
        This table tracks which users have liked which items so that
        likes can be toggled per user rather than incremented blindly.
        Schema:
          - user_id TEXT
          - item_id TEXT
          PRIMARY KEY (user_id, item_id)
        """
        query = """
            CREATE TABLE IF NOT EXISTS inventory_likes (
                user_id TEXT,
                item_id TEXT,
                PRIMARY KEY (user_id, item_id)
            );
        """
        self._execute(query)

    def create_users_table(self):
        """
        Create a Users table for storing basic account information.
        Schema:
          - id        UUID PRIMARY KEY
          - firstname TEXT
          - lastname  TEXT
          - email     TEXT
          - password  TEXT  (hashed)
          - phone     TEXT
          - usertype  TEXT  (e.g., 'customer', 'admin')
        """
        query = """
            CREATE TABLE IF NOT EXISTS users (
                id        TEXT PRIMARY KEY,
                firstname TEXT,
                lastname  TEXT,
                email     TEXT UNIQUE,
                password  TEXT,
                phone     TEXT,
                usertype  TEXT
            );
        """
        self._execute(query)

    def _ensure_images_table(self):
        """
        Ensure the helper table for additional images exists.
        """
        query = """
            CREATE TABLE IF NOT EXISTS inventory_images (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id   TEXT,
                image_url TEXT
            );
        """
        self._execute(query)
        # Helpful index for lookups by item_id
        self._execute(
            "CREATE INDEX IF NOT EXISTS idx_inventory_images_item ON inventory_images(item_id);"
        )

    def _ensure_cart_items_table(self):
        """
        Ensure the cart_items table exists.
        Schema:
          - cart_id  UUID  (per-browser / per-session cart identifier)
          - item_id  UUID  (inventory item)
          - quantity INT   (usually 1 for furniture)
        """
        query = """
            CREATE TABLE IF NOT EXISTS cart_items (
                cart_id  TEXT,
                item_id  TEXT,
                quantity INTEGER,
                PRIMARY KEY (cart_id, item_id)
            );
        """
        self._execute(query)

    def _ensure_password_reset_table(self):
        """
        Ensure the password_reset_tokens table exists.
        Schema:
          - token      TEXT PRIMARY KEY
          - user_id    TEXT (references users.id)
          - expires_at TEXT (ISO-8601 timestamp)
        """
        query = """
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                token      TEXT PRIMARY KEY,
                user_id    TEXT,
                expires_at TEXT
            );
        """
        self._execute(query)

    def _ensure_orders_tables(self):
        """
        Ensure the Orders, ItemSold, and Payments tables exist for recording
        orders. Also migrate any legacy 'sales' table to 'orders'.
        """
        # Check for existing orders table
        cur = self._execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='orders';"
        )
        has_orders = cur.fetchone() is not None

        # Check for legacy sales table
        cur = self._execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='sales';"
        )
        has_sales = cur.fetchone() is not None

        # If there is a legacy 'sales' table but no 'orders' table, migrate
        if not has_orders and has_sales:
            # Rename table
            self._execute("ALTER TABLE sales RENAME TO orders;")
            # Attempt to rename primary key column sale_id -> order_id
            try:
                self._execute("ALTER TABLE orders RENAME COLUMN sale_id TO order_id;")
            except Exception:
                # If the SQLite runtime does not support RENAME COLUMN,
                # continue using the existing column name.
                pass

        # Orders table
        orders_query = """
            CREATE TABLE IF NOT EXISTS orders (
                order_id     TEXT PRIMARY KEY,
                user_id      TEXT,
                date         TEXT,
                subtotal     REAL,
                taxes        REAL,
                shipping_fee REAL,
                total        REAL,
                payment_id   TEXT
            );
        """
        self._execute(orders_query)

        # ItemSold table
        items_sold_query = """
            CREATE TABLE IF NOT EXISTS items_sold (
                sale_id TEXT,
                item_id TEXT
            );
        """
        self._execute(items_sold_query)

        # Payments table
        payments_query = """
            CREATE TABLE IF NOT EXISTS payments (
                payment_id                 TEXT PRIMARY KEY,
                sale_id                    TEXT,
                payment_method             TEXT,
                payment_confirmation_number TEXT
            );
        """
        self._execute(payments_query)

    # ------------------------------------------------------------------
    # Sales / Payments helpers
    # ------------------------------------------------------------------

    TAX_RATE = 0.065

    def create_sale(self, user_id: str | None, items, shipping_fee: float, payment_method: str,
                    payment_confirmation_number: str):
        """
        Create a sale record with associated line items and payment.

        `items` is an iterable of inventory rows (with at least id and price).
        """
        if not items:
            return None

        sale_id = str(uuid4())
        payment_id = str(uuid4())

        subtotal = 0.0
        for it in items:
            try:
                subtotal += float(getattr(it, "price", 0) or 0)
            except Exception:
                continue

        taxes = round(subtotal * self.TAX_RATE, 2)
        total = round(subtotal + taxes + float(shipping_fee or 0), 2)
        now = datetime.utcnow().isoformat()

        # Insert order (formerly 'sale')
        self._execute(
            """
            INSERT INTO orders (order_id, user_id, date, subtotal, taxes, shipping_fee, total, payment_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (sale_id, user_id, now, subtotal, taxes, shipping_fee, total, payment_id),
        )

        # Insert items_sold rows
        for it in items:
            item_id = str(getattr(it, "id", "") or "")
            if not item_id:
                continue
            self._execute(
                "INSERT INTO items_sold (sale_id, item_id) VALUES (?, ?);",
                (sale_id, item_id),
            )

        # Insert payment row
        self._execute(
            """
            INSERT INTO payments (payment_id, sale_id, payment_method, payment_confirmation_number)
            VALUES (?, ?, ?, ?);
            """,
            (payment_id, sale_id, payment_method, payment_confirmation_number),
        )

        return SimpleNamespace(
            sale_id=sale_id,
            payment_id=payment_id,
            subtotal=subtotal,
            taxes=taxes,
            shipping_fee=shipping_fee,
            total=total,
        )

    def get_recent_sales_for_user(self, user_id: str, limit: int = 5):
        """
        Backwards-compatible helper: return up to `limit` most recent sales.
        """
        return self.get_sales_for_user(user_id, limit=limit, offset=0)

    def count_sales_for_user(self, user_id: str) -> int:
        """
        Return the total number of order rows for a given user_id.
        """
        if not user_id:
            return 0
        cur = self._execute(
            "SELECT COUNT(*) AS cnt FROM orders WHERE user_id = ?;",
            (user_id,),
        )
        row = cur.fetchone()
        return int(getattr(row, "cnt", 0) or 0)

    def get_sales_for_user(self, user_id: str, limit: int, offset: int = 0):
        """
        Return a page of sales for a given user_id, ordered by date desc.
        """
        if not user_id or limit <= 0:
            return []
        if offset < 0:
            offset = 0
        query = """
            SELECT order_id AS sale_id, date, total
            FROM orders
            WHERE user_id = ?
            ORDER BY date DESC
            LIMIT ? OFFSET ?;
        """
        cur = self._execute(query, (user_id, limit, offset))
        return cur.fetchall()

    def get_sale_by_id(self, sale_id: str):
        """
        Fetch a single sale row by its primary key sale_id.
        """
        if not sale_id:
            return None
        cur = self._execute(
            "SELECT order_id AS sale_id, user_id, date, subtotal, taxes, shipping_fee, total, payment_id "
            "FROM orders WHERE order_id = ?;",
            (sale_id,),
        )
        return cur.fetchone()

    def get_items_for_sale(self, sale_id: str):
        """
        Return inventory item rows associated with a given sale, using the
        items_sold mapping table.
        """
        if not sale_id:
            return []
        query = """
            SELECT i.*
            FROM items_sold s
            JOIN inventory i ON s.item_id = i.id
            WHERE s.sale_id = ?;
        """
        cur = self._execute(query, (sale_id,))
        return cur.fetchall()

    # ------------------------------------------------------------------
    # Password reset token helpers
    # ------------------------------------------------------------------

    def create_password_reset_token(self, user_id: str, token: str, expires_at: str):
        """
        Store a password reset token for a user. Any existing token with the same
        value will be overwritten.
        """
        self._ensure_password_reset_table()
        query = """
            INSERT OR REPLACE INTO password_reset_tokens (token, user_id, expires_at)
            VALUES (?, ?, ?);
        """
        self._execute(query, (token, user_id, expires_at))

    def get_password_reset_token(self, token: str):
        """
        Fetch a password reset token row by token value.
        """
        self._ensure_password_reset_table()
        query = "SELECT token, user_id, expires_at FROM password_reset_tokens WHERE token = ?;"
        cur = self._execute(query, (token,))
        return cur.fetchone()

    def delete_password_reset_token(self, token: str):
        """
        Delete a single password reset token.
        """
        self._ensure_password_reset_table()
        self._execute("DELETE FROM password_reset_tokens WHERE token = ?;", (token,))

    # ------------------------------------------------------------------
    # Inventory operations
    # ------------------------------------------------------------------

    def insert_data(self, tablename, data):
        """
        Insert a new inventory row; returns the generated id.
        """
        item_id = str(uuid4())
        created_at = data.get("created_at") or datetime.now().isoformat()
        updated_at = data.get("updated_at") or created_at
        # Derive a category if not explicitly provided
        category = data.get("category") or self._infer_inventory_category(
            data.get("name", ""), data.get("description", "")
        )
        # Normalize pricing: 'price' is always the current selling price.
        price = float(data["price"])
        original_price_raw = data.get("original_price")
        original_price = None
        if original_price_raw not in (None, ""):
            try:
                original_price = float(original_price_raw)
            except (TypeError, ValueError):
                original_price = None
        # Only keep an original_price that is strictly greater than the
        # current selling price; otherwise treat as not discounted.
        if original_price is not None and original_price <= price:
            original_price = None

        query = f"""
            INSERT INTO {tablename} (id, name, price, original_price, description, image_url, created_at, updated_at, status, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        self._execute(
            query,
            (
                item_id,
                data["name"],
                price,
                original_price,
                data["description"],
                data["image_url"],
                created_at,
                updated_at,
                data["status"],
                category,
            ),
        )
        return item_id

    def get_all_data(self, tablename):
        query = f"SELECT * FROM {tablename};"
        cur = self._execute(query)
        return cur.fetchall()

    def get_item_by_id(self, tablename, item_id: str):
        """
        Fetch a single item by its UUID primary key.
        """
        query = f"SELECT * FROM {tablename} WHERE id = ?;"
        cur = self._execute(query, (item_id,))
        return cur.fetchone()

    def increment_item_view_count(self, tablename: str, item_id: str):
        """
        Increment the views counter for a single inventory item.
        """
        if not item_id:
            return
        query = f"""
            UPDATE {tablename}
            SET views = COALESCE(views, 0) + 1
            WHERE id = ?;
        """
        self._execute(query, (item_id,))

    def increment_item_like_count(self, tablename: str, item_id: str):
        """
        Increment the likes counter for a single inventory item.
        """
        if not item_id:
            return
        query = f"""
            UPDATE {tablename}
            SET likes = COALESCE(likes, 0) + 1
            WHERE id = ?;
        """
        self._execute(query, (item_id,))

    # ------------------------------------------------------------------
    # Per-user like helpers
    # ------------------------------------------------------------------

    def user_has_liked_item(self, user_id: str, item_id: str) -> bool:
        """
        Return True if the given user has liked the specified inventory item.
        """
        if not user_id or not item_id:
            return False
        self._ensure_inventory_likes_table()
        cur = self._execute(
            "SELECT 1 FROM inventory_likes WHERE user_id = ? AND item_id = ? LIMIT 1;",
            (user_id, item_id),
        )
        return cur.fetchone() is not None

    def add_like_for_item(self, user_id: str, item_id: str, tablename: str = "inventory"):
        """
        Record a like from a user for an item and increment the aggregate
        likes counter on the inventory row. If the user has already liked
        the item, this is a no-op.
        """
        if not user_id or not item_id:
            return
        self._ensure_inventory_likes_table()
        if self.user_has_liked_item(user_id, item_id):
            return
        # Record the per-user like
        self._execute(
            "INSERT INTO inventory_likes (user_id, item_id) VALUES (?, ?);",
            (user_id, item_id),
        )
        # Bump the aggregate like count
        self._execute(
            f"""
            UPDATE {tablename}
            SET likes = COALESCE(likes, 0) + 1
            WHERE id = ?;
            """,
            (item_id,),
        )

    def remove_like_for_item(self, user_id: str, item_id: str, tablename: str = "inventory"):
        """
        Remove a like from a user for an item and decrement the aggregate
        likes counter on the inventory row, never dropping below zero.
        If the user has not liked the item, this is a no-op.
        """
        if not user_id or not item_id:
            return
        self._ensure_inventory_likes_table()
        if not self.user_has_liked_item(user_id, item_id):
            return
        # Remove the per-user like
        self._execute(
            "DELETE FROM inventory_likes WHERE user_id = ? AND item_id = ?;",
            (user_id, item_id),
        )
        # Decrement the aggregate like count, clamping at 0
        self._execute(
            f"""
            UPDATE {tablename}
            SET likes = MAX(COALESCE(likes, 0) - 1, 0)
            WHERE id = ?;
            """,
            (item_id,),
        )

    def get_user_by_email(self, email: str):
        """
        Fetch a single user by email. Uses ALLOW FILTERING, which is fine for small datasets.
        """
        query = "SELECT * FROM users WHERE email = ?;"
        cur = self._execute(query, (email,))
        return cur.fetchone()

    def get_user_by_id(self, user_id: str):
        """
        Fetch a single user by primary key id.
        """
        query = "SELECT * FROM users WHERE id = ?;"
        cur = self._execute(query, (user_id,))
        return cur.fetchone()

    def get_all_users(self):
        """
        Return all users in the system, ordered by last name then first name.
        """
        query = "SELECT * FROM users ORDER BY lastname, firstname;"
        cur = self._execute(query)
        return cur.fetchall()

    def search_users(self, query_text: str):
        """
        Search users by partial match on firstname, lastname, email, phone, or usertype.
        """
        like = f"%{query_text}%"
        query = """
            SELECT *
            FROM users
            WHERE firstname LIKE ?
               OR lastname LIKE ?
               OR email LIKE ?
               OR phone LIKE ?
               OR usertype LIKE ?
            ORDER BY lastname, firstname;
        """
        cur = self._execute(query, (like, like, like, like, like))
        return cur.fetchall()

    def insert_user(self, firstname: str, lastname: str, email: str, password_hash: str,
                    phone: str = None, usertype: str = "customer"):
        """
        Insert a new user into the users table.
        Assumes the password is already hashed.
        """
        user_id = str(uuid4())
        query = """
            INSERT INTO users (id, firstname, lastname, email, password, phone, usertype)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        self._execute(
            query,
            (
                user_id,
                firstname,
                lastname,
                email,
                password_hash,
                phone,
                usertype,
            ),
        )
        return user_id

    # ------------------------------------------------------------------
    # Inventory category helpers
    # ------------------------------------------------------------------

    def _infer_inventory_category(self, name: str, description: str) -> str:
        """
        Infer a coarse category for an inventory item based on its name/description.

        Categories:
          - "Bedroom"
          - "Living Room"
          - "Study"
          - "Other"
        """
        text = f"{name or ''} {description or ''}".lower()

        bedroom_keywords = [
            "nightstand",
            "bedside",
            "bed room",
            "bedroom",
            "dresser",
            "chest",
            "armoire",
        ]
        living_keywords = [
            "sofa",
            "couch",
            "coffee table",
            "side table",
            "end table",
            "console",
            "console table",
            "tv stand",
            "media console",
            "pedestal table",
            "accent table",
            "bench",
            "stool",
            "chair",
            "recliner",
        ]
        study_keywords = [
            "desk",
            "writing desk",
            "office",
            "study",
            "bookcase",
            "bookshelf",
        ]

        def contains_any(keywords):
            return any(k in text for k in keywords)

        if contains_any(bedroom_keywords):
            return "Bedroom"
        if contains_any(living_keywords):
            return "Living Room"
        if contains_any(study_keywords):
            return "Study"
        return "Other"

    def backfill_inventory_categories(self):
        """
        Populate the category column for existing inventory rows that lack it.
        """
        # Ensure the column exists before attempting to backfill
        self._ensure_inventory_category_column()
        cur = self._execute("SELECT id, name, description, category FROM inventory;")
        rows = cur.fetchall()
        for row in rows:
            current = getattr(row, "category", None)
            if current:
                continue
            category = self._infer_inventory_category(
                getattr(row, "name", "") or "",
                getattr(row, "description", "") or "",
            )
            self._execute(
                "UPDATE inventory SET category = ? WHERE id = ?;",
                (category, row.id),
            )

    def update_user(self, user_id: str, data: dict):
        """
        Update fields for an existing user. The `data` dict may include any of:
        firstname, lastname, email, phone, usertype, password (already hashed).
        Only provided fields will be updated.
        """
        if not data:
            return

        allowed_keys = {"firstname", "lastname", "email", "phone", "usertype", "password"}
        set_clauses = []
        params = []
        for key, value in data.items():
            if key in allowed_keys:
                set_clauses.append(f"{key} = ?")
                params.append(value)

        if not set_clauses:
            return

        sql = f"UPDATE users SET {', '.join(set_clauses)} WHERE id = ?;"
        params.append(user_id)
        self._execute(sql, tuple(params))

    def update_item(self, tablename, item_id: str, data: dict):
        """
        Update an existing item in the given table.
        """
        updated_at = datetime.now().isoformat()
        # Preserve existing category unless explicitly provided; if not present,
        # recompute based on the updated name/description.
        existing = self.get_item_by_id(tablename, item_id)
        current_category = getattr(existing, "category", None) if existing else None
        new_category = data.get(
            "category",
            current_category
            or self._infer_inventory_category(
                data.get("name", getattr(existing, "name", "")),
                data.get("description", getattr(existing, "description", "")),
            ),
        )
        # Normalize pricing: 'price' is the current selling price.
        price = float(data["price"])
        original_price_raw = data.get("original_price")
        original_price = None
        if original_price_raw not in (None, ""):
            try:
                original_price = float(original_price_raw)
            except (TypeError, ValueError):
                original_price = None
        if original_price is not None and original_price <= price:
            original_price = None
        query = f"""
            UPDATE {tablename}
            SET name = ?,
                price = ?,
                original_price = ?,
                description = ?,
                image_url = ?,
                status = ?,
                category = ?,
                updated_at = ?
            WHERE id = ?;
        """
        self._execute(
            query,
            (
                data["name"],
                price,
                original_price,
                data["description"],
                data["image_url"],
                data["status"],
                new_category,
                updated_at,
                item_id,
            ),
        )

    def mark_items_pending(self, tablename, item_ids):
        """
        Mark the given items as pending (reserved/in cart or awaiting pickup)
        without changing other fields.
        """
        if not item_ids:
            return
        now = datetime.now().isoformat()
        query = f"""
            UPDATE {tablename}
            SET status = ?,
                updated_at = ?
            WHERE id = ?;
        """
        for item_id in item_ids:
            self._execute(
                query,
                ("pending", now, item_id),
            )

    def mark_items_sold(self, tablename, item_ids):
        """
        Mark the given items as sold (payment completed) without changing
        other fields.
        """
        if not item_ids:
            return
        now = datetime.now().isoformat()
        query = f"""
            UPDATE {tablename}
            SET status = ?,
                updated_at = ?
            WHERE id = ?;
        """
        for item_id in item_ids:
            self._execute(
                query,
                ("sold", now, item_id),
            )

    def mark_items_available(self, tablename, item_ids):
        """
        Mark the given items as available again (e.g., removed from all carts)
        without changing other fields.
        """
        if not item_ids:
            return
        now = datetime.now().isoformat()
        query = f"""
            UPDATE {tablename}
            SET status = ?,
                updated_at = ?
            WHERE id = ?;
        """
        for item_id in item_ids:
            self._execute(
                query,
                ("available", now, item_id),
            )

    def delete_inventory_item(self, item_id: str):
        """
        Permanently delete an inventory item and its related records.

        This removes:
          - any cart rows referencing the item
          - any additional images for the item
          - any per-user like records for the item
          - the item itself from the inventory table
        """
        if not item_id:
            return

        # Remove from all carts
        self._ensure_cart_items_table()
        self._execute(
            "DELETE FROM cart_items WHERE item_id = ?;",
            (item_id,),
        )

        # Remove any additional images
        self._ensure_images_table()
        self._execute(
            "DELETE FROM inventory_images WHERE item_id = ?;",
            (item_id,),
        )

        # Remove per-user like records
        self._ensure_inventory_likes_table()
        self._execute(
            "DELETE FROM inventory_likes WHERE item_id = ?;",
            (item_id,),
        )

        # Finally, delete the inventory row itself
        self._execute(
            "DELETE FROM inventory WHERE id = ?;",
            (item_id,),
        )

    def get_images_for_item(self, item_id: str):
        """
        Return a list of all image URLs associated with an item.
        """
        self._ensure_images_table()
        cur = self._execute(
            "SELECT image_url FROM inventory_images WHERE item_id = ?;",
            (item_id,),
        )
        rows = cur.fetchall()
        return [row.image_url for row in rows]

    def set_images_for_item(self, item_id: str, images):
        """
        Replace all images for an item with the provided list of URLs.
        """
        self._ensure_images_table()
        # Clear existing images for this item
        self._execute(
            "DELETE FROM inventory_images WHERE item_id = ?;",
            (item_id,),
        )
        # Insert new set
        for url in images:
            self._execute(
                "INSERT INTO inventory_images (item_id, image_url) VALUES (?, ?);",
                (item_id, url),
            )

    # -----------------------
    # Cart helper operations
    # -----------------------

    def get_cart_items(self, cart_id: str):
        """
        Return all (item_id, quantity) rows for a given cart.
        """
        self._ensure_cart_items_table()
        cur = self._execute(
            "SELECT item_id, quantity FROM cart_items WHERE cart_id = ?;",
            (cart_id,),
        )
        return cur.fetchall()

    def get_cart_item_count(self, cart_id: str) -> int:
        """
        Return how many distinct items are in the cart.
        """
        return len(self.get_cart_items(cart_id))

    def is_item_in_cart(self, cart_id: str, item_id: str) -> bool:
        """
        Check whether a given item is already in the cart.
        """
        self._ensure_cart_items_table()
        cur = self._execute(
            "SELECT quantity FROM cart_items WHERE cart_id = ? AND item_id = ? LIMIT 1;",
            (cart_id, item_id),
        )
        return cur.fetchone() is not None

    def add_item_to_cart(self, cart_id: str, item_id: str, quantity: int = 1, ttl_seconds: int | None = None):
        """
        Add or update an item in the cart. For furniture, quantity will usually stay at 1.
        If ttl_seconds is provided, the row will expire after that many seconds.
        """
        self._ensure_cart_items_table()
        # TTL semantics are ignored for SQLite; the row will persist until removed
        query = """
            INSERT OR REPLACE INTO cart_items (cart_id, item_id, quantity)
            VALUES (?, ?, ?);
        """
        self._execute(query, (cart_id, item_id, quantity))

    def remove_item_from_cart(self, cart_id: str, item_id: str):
        """
        Remove a single item from the cart.
        """
        self._ensure_cart_items_table()
        self._execute(
            "DELETE FROM cart_items WHERE cart_id = ? AND item_id = ?;",
            (cart_id, item_id),
        )

    def clear_cart(self, cart_id: str):
        """
        Remove all items from the cart.
        """
        self._ensure_cart_items_table()
        self._execute(
            "DELETE FROM cart_items WHERE cart_id = ?;",
            (cart_id,),
        )

    def normalize_cart_items(self, cart_id: str):
        """
        Re-insert all items in a cart without TTL so they become long-lived.
        Useful when promoting a guest cart to a logged-in user's cart.
        """
        # TTL is not used with SQLite, so there is nothing to normalize.
        return

    def item_is_in_any_cart(self, item_id: str) -> bool:
        """
        Return True if the given item_id exists in any cart.
        Uses ALLOW FILTERING which is acceptable for this small dataset.
        """
        self._ensure_cart_items_table()
        cur = self._execute(
            "SELECT cart_id FROM cart_items WHERE item_id = ? LIMIT 1;",
            (item_id,),
        )
        return cur.fetchone() is not None

    # ------------------------------------------------------------------
    # Category management helpers
    # ------------------------------------------------------------------

    def get_all_categories(self) -> list[str]:
        """
        Return a sorted list of distinct, non-empty inventory categories.
        """
        cur = self._execute(
            "SELECT DISTINCT category FROM inventory WHERE category IS NOT NULL AND TRIM(category) != '';"
        )
        rows = cur.fetchall()
        # rows are SimpleNamespace with attribute 'category'
        cats = [str(getattr(row, "category", "")).strip() for row in rows]
        # De-duplicate and sort for stable display
        unique = sorted({c for c in cats if c})
        return unique

    def rename_category(self, old_name: str, new_name: str):
        """
        Rename a category for all inventory items.
        """
        if not old_name or not new_name:
            return
        now = datetime.now().isoformat()
        self._execute(
            """
            UPDATE inventory
            SET category = ?, updated_at = ?
            WHERE category = ?;
            """,
            (new_name, now, old_name),
        )

    def delete_category_and_reassign(self, category_name: str, fallback: str = "Other"):
        """
        Delete a category by reassigning any items currently using it to `fallback`
        (default \"Other\").
        """
        if not category_name:
            return
        fallback = fallback or "Other"
        now = datetime.now().isoformat()
        self._execute(
            """
            UPDATE inventory
            SET category = ?, updated_at = ?
            WHERE category = ?;
            """,
            (fallback, now, category_name),
        )

    def add_category_if_missing(self, category_name: str):
        """
        Ensure the given category name exists somewhere in the inventory table by
        inserting a placeholder row only if absolutely necessary.

        NOTE: For this app we treat \"categories\" as the distinct set of values
        used by existing inventory items. Creating a brand new category is as
        simple as assigning that category to at least one item. This helper is
        kept for future extensibility but is not used by the current admin UI.
        """
        if not category_name:
            return
        # If there is at least one row already using this category, nothing to do.
        cur = self._execute(
            "SELECT 1 AS has_row FROM inventory WHERE category = ? LIMIT 1;",
            (category_name,),
        )
        row = cur.fetchone()
        if row is not None:
            return
        # Otherwise insert a minimal placeholder row so that the category appears.
        # This avoids adding a separate categories table while still letting admins
        # seed new categories if desired.
        item_id = str(uuid4())
        now = datetime.now().isoformat()
        self._execute(
            """
            INSERT INTO inventory (id, name, price, description, image_url, created_at, updated_at, status, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                item_id,
                f"Category placeholder: {category_name}",
                0.0,
                "",
                "",
                now,
                now,
                "unlisted",
                category_name,
            ),
        )
