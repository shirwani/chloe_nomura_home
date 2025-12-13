from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
from uuid import UUID, uuid4


class Cassandra:
    def __init__(self):
        self.cluster = Cluster(
            ['127.0.0.1'],
            auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
        )
        self.session = self.cluster.connect('chloe')

    def shutdown(self):
        self.cluster.shutdown()

    def create_inventory_table(self):
        query = f"""
            CREATE TABLE IF NOT EXISTS inventory (
                id              UUID PRIMARY KEY,
                name            TEXT,
                price           FLOAT,
                description     TEXT,
                image_url       TEXT,
                created_at      TIMESTAMP,
                updated_at      TIMESTAMP,
                status          TEXT
            );       
        """
        self.session.execute(query)

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
                id        uuid PRIMARY KEY,
                firstname text,
                lastname  text,
                email     text,
                password  text,
                phone     text,
                usertype  text
            );
        """
        self.session.execute(query)

    def _ensure_images_table(self):
        """
        Ensure the helper table for additional images exists.
        """
        query = """
            CREATE TABLE IF NOT EXISTS inventory_images (
                item_id   UUID,
                image_url TEXT,
                PRIMARY KEY (item_id, image_url)
            );
        """
        self.session.execute(query)

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
                cart_id uuid,
                item_id uuid,
                quantity int,
                PRIMARY KEY (cart_id, item_id)
            );
        """
        self.session.execute(query)

    def insert_data(self, tablename, data):
        query = f"""
            INSERT INTO {tablename} (id, name, price, description, image_url, created_at, updated_at, status)
            VALUES (uuid(), %(name)s, %(price)s, %(description)s, %(image_url)s, %(created_at)s, %(updated_at)s, %(status)s);
        """
        self.session.execute(query, {
            'name': data['name'],
            'price': data['price'],
            'description': data['description'],
            'image_url': data['image_url'],
            'created_at': data['created_at'],
            'updated_at': data['updated_at'],
            'status': data['status'],
        })

    def get_all_data(self, tablename):
        query = f"""
            SELECT * FROM {tablename};
        """
        return self.session.execute(query)

    def get_item_by_id(self, tablename, item_id: str):
        """
        Fetch a single item by its UUID primary key.
        """
        query = f"SELECT * FROM {tablename} WHERE id = %s;"
        result = self.session.execute(query, (UUID(item_id),)).one()
        return result

    def get_user_by_email(self, email: str):
        """
        Fetch a single user by email. Uses ALLOW FILTERING, which is fine for small datasets.
        """
        query = "SELECT * FROM users WHERE email = %s ALLOW FILTERING;"
        return self.session.execute(query, (email,)).one()

    def insert_user(self, firstname: str, lastname: str, email: str, password_hash: str,
                    phone: str = None, usertype: str = "customer"):
        """
        Insert a new user into the users table.
        Assumes the password is already hashed.
        """
        user_id = uuid4()
        query = """
            INSERT INTO users (id, firstname, lastname, email, password, phone, usertype)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """
        self.session.execute(
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
        query = f"""
            UPDATE {tablename}
            SET name = %s,
                price = %s,
                description = %s,
                image_url = %s,
                status = %s,
                updated_at = %s
            WHERE id = %s;
        """
        self.session.execute(
            query,
            (
                data["name"],
                float(data["price"]),
                data["description"],
                data["image_url"],
                data["status"],
                datetime.now(),
                UUID(item_id),
            ),
        )

    def mark_items_sold(self, tablename, item_ids):
        """
        Mark the given items as pending (payment received, awaiting pickup)
        without changing other fields.
        """
        if not item_ids:
            return
        now = datetime.now()
        query = f"""
            UPDATE {tablename}
            SET status = %s,
                updated_at = %s
            WHERE id = %s;
        """
        for item_id in item_ids:
            self.session.execute(
                query,
                ("pending", now, UUID(item_id)),
            )

    def mark_items_available(self, tablename, item_ids):
        """
        Mark the given items as available again (e.g., removed from all carts)
        without changing other fields.
        """
        if not item_ids:
            return
        now = datetime.now()
        query = f"""
            UPDATE {tablename}
            SET status = %s,
                updated_at = %s
            WHERE id = %s;
        """
        for item_id in item_ids:
            self.session.execute(
                query,
                ("available", now, UUID(item_id)),
            )

    def get_images_for_item(self, item_id: str):
        """
        Return a list of all image URLs associated with an item.
        """
        self._ensure_images_table()
        rows = self.session.execute(
            "SELECT image_url FROM inventory_images WHERE item_id = %s;",
            (UUID(item_id),),
        )
        return [row.image_url for row in rows]

    def set_images_for_item(self, item_id: str, images):
        """
        Replace all images for an item with the provided list of URLs.
        """
        self._ensure_images_table()
        # Clear existing images for this item
        self.session.execute(
            "DELETE FROM inventory_images WHERE item_id = %s;",
            (UUID(item_id),),
        )
        # Insert new set
        for url in images:
            self.session.execute(
                "INSERT INTO inventory_images (item_id, image_url) VALUES (%s, %s);",
                (UUID(item_id), url),
            )

    # -----------------------
    # Cart helper operations
    # -----------------------

    def get_cart_items(self, cart_id: str):
        """
        Return all (item_id, quantity) rows for a given cart.
        """
        self._ensure_cart_items_table()
        rows = self.session.execute(
            "SELECT item_id, quantity FROM cart_items WHERE cart_id = %s;",
            (UUID(cart_id),),
        )
        return list(rows)

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
        row = self.session.execute(
            "SELECT quantity FROM cart_items WHERE cart_id = %s AND item_id = %s;",
            (UUID(cart_id), UUID(item_id)),
        ).one()
        return row is not None

    def add_item_to_cart(self, cart_id: str, item_id: str, quantity: int = 1, ttl_seconds: int | None = None):
        """
        Add or update an item in the cart. For furniture, quantity will usually stay at 1.
        If ttl_seconds is provided, the row will expire after that many seconds.
        """
        self._ensure_cart_items_table()
        if ttl_seconds:
            query = "INSERT INTO cart_items (cart_id, item_id, quantity) VALUES (%s, %s, %s) USING TTL %s;"
            params = (UUID(cart_id), UUID(item_id), quantity, int(ttl_seconds))
        else:
            query = "INSERT INTO cart_items (cart_id, item_id, quantity) VALUES (%s, %s, %s);"
            params = (UUID(cart_id), UUID(item_id), quantity)
        self.session.execute(query, params)

    def remove_item_from_cart(self, cart_id: str, item_id: str):
        """
        Remove a single item from the cart.
        """
        self._ensure_cart_items_table()
        self.session.execute(
            "DELETE FROM cart_items WHERE cart_id = %s AND item_id = %s;",
            (UUID(cart_id), UUID(item_id)),
        )

    def clear_cart(self, cart_id: str):
        """
        Remove all items from the cart.
        """
        self._ensure_cart_items_table()
        self.session.execute(
            "DELETE FROM cart_items WHERE cart_id = %s;",
            (UUID(cart_id),),
        )

    def normalize_cart_items(self, cart_id: str):
        """
        Re-insert all items in a cart without TTL so they become long-lived.
        Useful when promoting a guest cart to a logged-in user's cart.
        """
        rows = self.get_cart_items(cart_id)
        for row in rows:
            quantity = getattr(row, "quantity", 1) or 1
            # Re-write the row without TTL to clear any previous TTL
            self.add_item_to_cart(cart_id, str(row.item_id), quantity=quantity, ttl_seconds=None)

    def item_is_in_any_cart(self, item_id: str) -> bool:
        """
        Return True if the given item_id exists in any cart.
        Uses ALLOW FILTERING which is acceptable for this small dataset.
        """
        self._ensure_cart_items_table()
        rows = self.session.execute(
            "SELECT cart_id FROM cart_items WHERE item_id = %s ALLOW FILTERING;",
            (UUID(item_id),),
        )
        for _ in rows:
            return True
        return False

if __name__ == '__main__':
    cassandra = Cassandra()
    # Ensure tables exist
    cassandra.create_inventory_table()
    cassandra.create_users_table()
    cassandra._ensure_images_table()
    cassandra._ensure_cart_items_table()

    now = datetime.now()

    seed_items = [
        {
            "name": "Set of beautiful nightstands",
            "price": 175,
            "description": """Gorgeous set of nightstands in black with bronze pulls.
Perfect beside a bed or sofa. Dimensions: 24 x 18 x 30. Local pickup in Tyngsboro, MA.""",
            "images": [
                "./static/images/black_nightstands_scene1.png",
                "./static/images/black_nightstands_scene2.png",
                "./static/images/black_nightstands_scene3.png",
            ],
        },
        {
            "name": "Mid-Century Walnut Side Table",
            "price": 220,
            "description": """Solid walnut side table with tapered legs and lower shelf.
Perfect next to a reading chair or sofa.""",
            "images": [
                "./static/images/walnut_side_table_scene1.png",
                "./static/images/walnut_side_table_scene2.png",
                "./static/images/walnut_side_table_scene3.png",
            ],
        },
        {
            "name": "Rustic Oak Coffee Table",
            "price": 340,
            "description": """Low-profile rustic oak coffee table with chunky legs and smooth top.
Beautiful centerpiece for a living room.""",
            "images": [
                "./static/images/oak_coffee_table_scene1.png",
                "./static/images/oak_coffee_table_scene2.png",
                "./static/images/oak_coffee_table_scene3.png",
            ],
        },
        {
            "name": "Farmhouse Console Table",
            "price": 295,
            "description": """Long farmhouse console table with turned legs and lower shelf.
Works great in entryways or behind a sofa.""",
            "images": [
                "./static/images/farmhouse_console_scene1.png",
                "./static/images/farmhouse_console_scene2.png",
                "./static/images/farmhouse_console_scene3.png",
            ],
        },
        {
            "name": "Pair of Spindle-Back Dining Chairs",
            "price": 180,
            "description": """Set of two solid wood spindle-back dining chairs in a warm honey finish.
Comfortable and sturdy.""",
            "images": [
                "./static/images/spindle_chairs_scene1.png",
                "./static/images/spindle_chairs_scene2.png",
                "./static/images/spindle_chairs_scene3.png",
            ],
        },
        {
            "name": "Whitewashed Nightstand with Drawer",
            "price": 165,
            "description": """Whitewashed solid wood nightstand with single drawer and open shelf.
Soft, coastal-inspired finish.""",
            "images": [
                "./static/images/whitewashed_nightstand_scene1.png",
                "./static/images/whitewashed_nightstand_scene2.png",
                "./static/images/whitewashed_nightstand_scene3.png",
            ],
        },
        {
            "name": "Round Pedestal Side Table",
            "price": 210,
            "description": """Round pedestal side table in rich espresso stain.
Great between two accent chairs or as a plant stand.""",
            "images": [
                "./static/images/round_pedestal_table_scene1.png",
                "./static/images/round_pedestal_table_scene2.png",
                "./static/images/round_pedestal_table_scene3.png",
            ],
        },
        {
            "name": "Reclaimed Wood Coffee Table",
            "price": 385,
            "description": """Reclaimed wood coffee table with visible grain and character.
Metal base provides a modern industrial touch.""",
            "images": [
                "./static/images/reclaimed_coffee_table_scene1.png",
                "./static/images/reclaimed_coffee_table_scene2.png",
                "./static/images/reclaimed_coffee_table_scene3.png",
            ],
        },
        {
            "name": "Slim Entryway Console Table",
            "price": 260,
            "description": """Slim solid wood console table ideal for narrow hallways.
Includes two small drawers for keys and mail.""",
            "images": [
                "./static/images/slim_console_scene1.png",
                "./static/images/slim_console_scene2.png",
                "./static/images/slim_console_scene3.png",
            ],
        },
        {
            "name": "Set of Ladder-Back Chairs",
            "price": 310,
            "description": """Set of four ladder-back dining chairs with woven rush seats.
Classic farmhouse look with updated finish.""",
            "images": [
                "./static/images/ladder_back_chairs_scene1.png",
                "./static/images/ladder_back_chairs_scene2.png",
                "./static/images/ladder_back_chairs_scene3.png",
            ],
        },
        {
            "name": "Two-Tone Coffee Table with Shelf",
            "price": 275,
            "description": """Two-tone coffee table with natural wood top and painted base.
Lower shelf provides extra storage for baskets or books.""",
            "images": [
                "./static/images/two_tone_coffee_table_scene1.png",
                "./static/images/two_tone_coffee_table_scene2.png",
                "./static/images/two_tone_coffee_table_scene3.png",
            ],
        },
    ]

    for data in seed_items:
        item_id = uuid4()
        primary_image = data["images"][0]
        cassandra.session.execute(
            """
            INSERT INTO inventory (id, name, price, description, image_url, created_at, updated_at, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                item_id,
                data["name"],
                float(data["price"]),
                data["description"],
                primary_image,
                now,
                now,
                "available",
            ),
        )
        cassandra.set_images_for_item(str(item_id), data["images"])

    cassandra.shutdown()
