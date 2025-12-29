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
        self.create_users_table()
        self._ensure_images_table()
        self._ensure_cart_items_table()
        self._ensure_password_reset_table()
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
                description TEXT,
                image_url   TEXT,
                created_at  TEXT,
                updated_at  TEXT,
                status      TEXT
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
        query = f"""
            INSERT INTO {tablename} (id, name, price, description, image_url, created_at, updated_at, status, category)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        self._execute(
            query,
            (
                item_id,
                data["name"],
                float(data["price"]),
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
        query = f"""
            UPDATE {tablename}
            SET name = ?,
                price = ?,
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
                float(data["price"]),
                data["description"],
                data["image_url"],
                data["status"],
                new_category,
                updated_at,
                item_id,
            ),
        )

    def mark_items_sold(self, tablename, item_ids):
        """
        Mark the given items as pending (payment received, awaiting pickup)
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
