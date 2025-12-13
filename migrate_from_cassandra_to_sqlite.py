"""
One-time migration script to move data from a local Cassandra keyspace
into the new SQLite database (chloe.db) used by this app.

Usage:
  1) Make sure Cassandra is running and the original keyspace 'chloe'
     (with tables inventory, inventory_images, users, cart_items) is available.
  2) In your virtualenv with cassandra-driver installed, run:

        python migrate_from_cassandra_to_sqlite.py

  3) After verifying the data in chloe.db, you can shut down Cassandra.
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

from datetime import datetime

from database import DBInterface, DB_PATH


def dt_to_iso(value):
  if isinstance(value, datetime):
      return value.isoformat()
  return value


def main():
    # --- Connect to source Cassandra ---
    cluster = Cluster(
        ["127.0.0.1"],
        auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
    )
    session = cluster.connect("chloe")

    # --- Connect to target SQLite via our DBInterface wrapper ---
    db = DBInterface(DB_PATH)

    # Optional: clear existing SQLite tables so migration is clean/overwrite
    for table in ["cart_items", "inventory_images", "inventory", "users"]:
        db._execute(f"DELETE FROM {table};")

    # -------------------------
    # Migrate inventory table
    # -------------------------
    inv_rows = session.execute("SELECT * FROM inventory;")
    for row in inv_rows:
        # Preserve original UUID-based id as a string
        inv_id = str(row.id)
        db._execute(
            """
            INSERT OR REPLACE INTO inventory
                (id, name, price, description, image_url, created_at, updated_at, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                inv_id,
                getattr(row, "name", None),
                float(getattr(row, "price", 0.0) or 0.0),
                getattr(row, "description", None),
                getattr(row, "image_url", None),
                dt_to_iso(getattr(row, "created_at", None)),
                dt_to_iso(getattr(row, "updated_at", None)),
                getattr(row, "status", None),
            ),
        )

    # ------------------------------
    # Migrate inventory_images table
    # ------------------------------
    img_rows = session.execute("SELECT item_id, image_url FROM inventory_images;")
    for row in img_rows:
        db._execute(
            "INSERT INTO inventory_images (item_id, image_url) VALUES (?, ?);",
            (str(row.item_id), getattr(row, "image_url", None)),
        )

    # -------------------------
    # Migrate users table
    # -------------------------
    user_rows = session.execute("SELECT * FROM users;")
    for row in user_rows:
        user_id = str(row.id)
        db._execute(
            """
            INSERT OR REPLACE INTO users
                (id, firstname, lastname, email, password, phone, usertype)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                user_id,
                getattr(row, "firstname", None),
                getattr(row, "lastname", None),
                getattr(row, "email", None),
                getattr(row, "password", None),
                getattr(row, "phone", None),
                getattr(row, "usertype", None),
            ),
        )

    # -------------------------
    # Migrate cart_items table
    # -------------------------
    cart_rows = session.execute("SELECT cart_id, item_id, quantity FROM cart_items;")
    for row in cart_rows:
        db._execute(
            """
            INSERT OR REPLACE INTO cart_items (cart_id, item_id, quantity)
            VALUES (?, ?, ?);
            """,
            (
                str(row.cart_id),
                str(row.item_id),
                int(getattr(row, "quantity", 1) or 1),
            ),
        )

    # --- Clean up ---
    db.shutdown()
    cluster.shutdown()
    print(f"Migration complete. Data copied into SQLite DB at: {DB_PATH}")


if __name__ == "__main__":
    main()


