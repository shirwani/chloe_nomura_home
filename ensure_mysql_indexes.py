import os

import mysql.connector


def main():
    host = os.environ["DB_HOST"]
    port = int(os.environ["DB_PORT"])
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    database = os.environ.get("DB_NAME", "chloe_home_test")
    ssl_ca = os.environ.get("DB_SSL_CA")

    kwargs = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }
    if ssl_ca:
        kwargs["ssl_ca"] = ssl_ca

    conn = mysql.connector.connect(**kwargs)
    cur = conn.cursor()

    print(f"Connected to {database} on {host}:{port} as {user}")

    statements = [
        # Orders: speed up lookups by user_id
        "ALTER TABLE orders ADD INDEX idx_orders_user_id (user_id);",
        # Inventory: support filters by status/category/price/views/likes
        "ALTER TABLE inventory ADD INDEX idx_inventory_status_category (status, category);",
        "ALTER TABLE inventory ADD INDEX idx_inventory_price (price);",
        "ALTER TABLE inventory ADD INDEX idx_inventory_views_likes (views, likes);",
        # Password reset tokens: optional helper index on user_id
        "ALTER TABLE password_reset_tokens ADD INDEX idx_password_reset_user_id (user_id);",
    ]

    for stmt in statements:
        try:
            print(f"Executing: {stmt}")
            cur.execute(stmt)
        except mysql.connector.Error as exc:
            # Ignore duplicate-index errors to keep this script idempotent.
            if exc.errno in (1061, 1068):  # duplicate key / multiple primary key
                print(f"  Skipping (already exists): {exc.msg}")
            else:
                print(f"  Error executing statement: {exc}")

    conn.commit()
    cur.close()
    conn.close()
    print("Index creation complete.")


if __name__ == "__main__":
    main()


