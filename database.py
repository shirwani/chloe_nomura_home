import sqlite3
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4


DB_PATH = "chloe.db"


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
        self.create_users_table()
        self._ensure_images_table()
        self._ensure_cart_items_table()

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
        query = f"""
            INSERT INTO {tablename} (id, name, price, description, image_url, created_at, updated_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
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

    def update_item(self, tablename, item_id: str, data: dict):
        """
        Update an existing item in the given table.
        """
        updated_at = datetime.now().isoformat()
        query = f"""
            UPDATE {tablename}
            SET name = ?,
                price = ?,
                description = ?,
                image_url = ?,
                status = ?,
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
