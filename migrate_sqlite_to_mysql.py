import os
import sqlite3

import mysql.connector


SQLITE_PATH = os.environ.get("SQLITE_PATH", "chloe.db")


def get_sqlite_connection():
    conn = sqlite3.connect(SQLITE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def get_mysql_connection():
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

    return mysql.connector.connect(**kwargs)


def copy_table(sqlite_conn, mysql_conn, table_name, mysql_table_name=None):
    """
    Copy all rows from a SQLite table into the corresponding MySQL table.
    The schema (column names) must already be compatible.
    """
    mysql_table_name = mysql_table_name or table_name

    s_cur = sqlite_conn.cursor()
    # Discover column names from SQLite
    s_cur.execute(f"PRAGMA table_info({table_name});")
    cols_info = s_cur.fetchall()
    if not cols_info:
        print(f"[WARN] No columns found for table {table_name}; skipping.")
        return

    columns = [row["name"] for row in cols_info]
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    # Fetch all rows from SQLite
    s_cur.execute(f"SELECT {col_list} FROM {table_name};")
    rows = s_cur.fetchall()

    m_cur = mysql_conn.cursor()
    # Clear existing rows to avoid duplicates
    m_cur.execute(f"DELETE FROM {mysql_table_name};")

    count = 0
    for row in rows:
        values = [row[c] for c in columns]
        m_cur.execute(
            f"INSERT INTO {mysql_table_name} ({col_list}) VALUES ({placeholders})",
            values,
        )
        count += 1

    mysql_conn.commit()
    print(f"Copied {count} rows into {mysql_table_name}")


def main():
    print(f"Using SQLite source: {SQLITE_PATH}")
    sqlite_conn = get_sqlite_connection()
    mysql_conn = get_mysql_connection()

    try:
        print(
            f"Connected to MySQL target: {os.environ['DB_NAME']} "
            f"at {os.environ['DB_HOST']}:{os.environ['DB_PORT']} "
            f"as {os.environ['DB_USER']}"
        )

        # Order matters a bit for foreign keys; parents first, children later.
        tables_in_order = [
            "inventory",
            "users",
            "inventory_images",
            "inventory_likes",
            "cart_items",
            "orders",
            "items_sold",
            "payments",
            "password_reset_tokens",
        ]

        for t in tables_in_order:
            print(f"\nMigrating table: {t}")
            copy_table(sqlite_conn, mysql_conn, t)

        print("\nMigration completed successfully.")
    finally:
        try:
            sqlite_conn.close()
        except Exception:
            pass
        try:
            mysql_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()


