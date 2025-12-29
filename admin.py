import hashlib
import uuid
from datetime import datetime
from database import DBInterface

def seed_database():
    db = DBInterface(db_path="chloe.db")
    now_str = datetime.now().isoformat()

    seed_items = [
        {
            "name": "Set of beautiful nightstands",
            "price": 175,
            "description": """Gorgeous set of nightstands in black with bronze pulls.
Perfect beside a bed or sofa. Dimensions: 24 x 18 x 30. Local pickup in Tyngsboro, MA.""",
            "images": [
                "./static/_images/black_nightstands_scene1.jpg",
                "./static/_images/black_nightstands_scene2.jpg",
                "./static/_images/black_nightstands_scene3.jpg",
            ],
        },
        {
            "name": "Mid-Century Walnut Side Table",
            "price": 220,
            "description": """Solid walnut side table with tapered legs and lower shelf.
Perfect next to a reading chair or sofa.""",
            "images": [
                "./static/_images/walnut_side_table_scene1.jpg",
                "./static/_images/walnut_side_table_scene2.jpg",
                "./static/_images/walnut_side_table_scene3.jpg",
            ],
        },
        {
            "name": "Rustic Oak Coffee Table",
            "price": 340,
            "description": """Low-profile rustic oak coffee table with chunky legs and smooth top.
Beautiful centerpiece for a living room.""",
            "images": [
                "./static/_images/oak_coffee_table_scene1.jpg",
                "./static/_images/oak_coffee_table_scene2.jpg",
                "./static/_images/oak_coffee_table_scene3.jpg",
            ],
        },
        {
            "name": "Farmhouse Console Table",
            "price": 295,
            "description": """Long farmhouse console table with turned legs and lower shelf.
Works great in entryways or behind a sofa.""",
            "images": [
                "./static/_images/farmhouse_console_scene1.jpg",
                "./static/_images/farmhouse_console_scene2.jpg",
                "./static/_images/farmhouse_console_scene3.jpg",
            ],
        },
        {
            "name": "Pair of Spindle-Back Dining Chairs",
            "price": 180,
            "description": """Set of two solid wood spindle-back dining chairs in a warm honey finish.
Comfortable and sturdy.""",
            "images": [
                "./static/_images/spindle_chairs_scene1.jpg",
                "./static/_images/spindle_chairs_scene2.jpg",
                "./static/_images/spindle_chairs_scene3.jpg",
            ],
        },
        {
            "name": "Whitewashed Nightstand with Drawer",
            "price": 165,
            "description": """Whitewashed solid wood nightstand with single drawer and open shelf.
Soft, coastal-inspired finish.""",
            "images": [
                "./static/_images/whitewashed_nightstand_scene1.jpg",
                "./static/_images/whitewashed_nightstand_scene2.jpg",
                "./static/_images/whitewashed_nightstand_scene3.jpg",
            ],
        },
        {
            "name": "Round Pedestal Side Table",
            "price": 210,
            "description": """Round pedestal side table in rich espresso stain.
Great between two accent chairs or as a plant stand.""",
            "images": [
                "./static/_images/round_pedestal_table_scene1.jpg",
                "./static/_images/round_pedestal_table_scene2.jpg",
                "./static/_images/round_pedestal_table_scene3.jpg",
            ],
        },
        {
            "name": "Reclaimed Wood Coffee Table",
            "price": 385,
            "description": """Reclaimed wood coffee table with visible grain and character.
Metal base provides a modern industrial touch.""",
            "images": [
                "./static/_images/reclaimed_coffee_table_scene1.jpg",
                "./static/_images/reclaimed_coffee_table_scene2.jpg",
                "./static/_images/reclaimed_coffee_table_scene3.jpg",
            ],
        },
        {
            "name": "Slim Entryway Console Table",
            "price": 260,
            "description": """Slim solid wood console table ideal for narrow hallways.
Includes two small drawers for keys and mail.""",
            "images": [
                "./static/_images/slim_console_scene1.jpg",
                "./static/_images/slim_console_scene2.jpg",
                "./static/_images/slim_console_scene3.jpg",
            ],
        },
        {
            "name": "Set of Ladder-Back Chairs",
            "price": 310,
            "description": """Set of four ladder-back dining chairs with woven rush seats.
Classic farmhouse look with updated finish.""",
            "images": [
                "./static/_images/ladder_back_chairs_scene1.jpg",
                "./static/_images/ladder_back_chairs_scene2.jpg",
                "./static/_images/ladder_back_chairs_scene3.jpg",
            ],
        },
        {
            "name": "Two-Tone Coffee Table with Shelf",
            "price": 275,
            "description": """Two-tone coffee table with natural wood top and painted base.
Lower shelf provides extra storage for baskets or books.""",
            "images": [
                "./static/_images/two_tone_coffee_table_scene1.jpg",
                "./static/_images/two_tone_coffee_table_scene2.jpg",
                "./static/_images/two_tone_coffee_table_scene3.jpg",
            ],
        },
    ]

    for data in seed_items:
        primary_image = data["images"][0]
        item_id = db.insert_data(
            "inventory",
            {
                "name": data["name"],
                "price": data["price"],
                "description": data["description"],
                "image_url": primary_image,
                "created_at": now_str,
                "updated_at": now_str,
                "status": "available",
            },
        )
        db.set_images_for_item(item_id, data["images"])

    db.shutdown()


def create_user(firstname, lastname, email, phone, password, usertype):
    db = DBInterface(db_path="chloe.db")
    db.create_users_table()

    password_hash = hash_user_password(email, phone, password)
    db.insert_user(
        firstname=firstname,
        lastname=lastname,
        email=email,
        phone=phone,
        password_hash=password_hash,
        usertype=usertype,
    )
    db.shutdown()
    print("Admin user created successfully")    

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


if __name__ == '__main__':
    # seed_database()
    # create_user(firstname="Chloe", lastname="Nomura", email="chloenomura4@gmail.com", phone="617-555-1234", password="33Leland!", usertype="admin")
    create_user(firstname="Cashier", lastname="Shirwani", email="zakishirwani@gmail.com", phone="617-555-1234", password="33Leland!", usertype="cashier")
    
