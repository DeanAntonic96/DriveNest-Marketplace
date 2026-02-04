from flask import Flask, render_template, request, redirect, url_for, flash, session
import sqlite3
import os
import uuid
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from datetime import datetime, timedelta

# App metadata and storage settings.
APP_NAME = "User Auth"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USERS_DB_PATH = os.path.join(BASE_DIR, "Users.db")
CARS_DB_PATH = os.path.join(BASE_DIR, "Cars.db")
TABLE_NAME = "users"
CAR_TABLE = "cars"
CAR_IMAGE_TABLE = "car_images"
CAR_STATUS_ACTIVE = "active"
CAR_STATUS_COMPLETED = "completed"
FAVORITES_TABLE = "favorites"
RECENT_VIEWS_TABLE = "recent_views"
TRANSACTIONS_TABLE = "transactions"
RATINGS_TABLE = "transaction_ratings"
THREADS_TABLE = "message_threads"
MESSAGES_TABLE = "messages"
UPLOAD_DIR = os.path.join("static", "uploads")
DEFAULT_CAR_IMAGE = "uploads/default-car.jpg"
DEFAULT_CAR_GALLERY_SIZE = 4
ALLOWED_MAKES = {
    "Audi": ["A3", "A4", "A6", "Q3", "Q5", "Q7"],
    "BMW": ["1 Series", "3 Series", "5 Series", "X1", "X3", "X5"],
    "Mercedes-Benz": ["A-Class", "C-Class", "E-Class", "S-Class", "GLA", "GLE"],
    "Volkswagen": ["Golf", "Passat", "Tiguan", "Touareg", "Polo"],
    "Tesla": ["Model 3", "Model S", "Model X", "Model Y"],
    "Skoda": ["Octavia", "Superb", "Kodiaq", "Karoq", "Fabia"],
    "Volvo": ["XC40", "XC60", "XC90", "S60", "S90"],
    "Peugeot": ["208", "308", "3008", "508"],
    "Hyundai": ["i20", "i30", "Tucson", "Santa Fe", "Ioniq 5"],
    "Renault": ["Clio", "Megane", "Captur", "Kadjar"],
    "Opel": ["Astra", "Corsa", "Insignia", "Mokka"],
    "Seat": ["Ibiza", "Leon", "Ateca", "Arona"],
}
ALLOWED_COLORS = {
    "Black",
    "White",
    "Gray",
    "Silver",
    "Blue",
    "Red",
    "Green",
    "Yellow",
    "Brown",
    "Orange",
}
ALLOWED_FUELS = {"Gasoline", "Diesel", "Hybrid", "Electric", "LPG"}
ALLOWED_TRANSMISSIONS = {"Automatic", "Manual", "Semi-automatic"}
ALLOWED_BODY_STYLES = {
    "Sedan",
    "SUV",
    "Hatchback",
    "Coupe",
    "Convertible",
    "Wagon",
    "Pickup",
    "Van",
}
MIN_YEAR = 1985
MAX_YEAR = 2026

# Create Flask app instance.
app = Flask(__name__)
app.config["SECRET_KEY"] = "change-me-in-production"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(minutes=3)
app.config["MAX_IMAGE_BYTES"] = 1 * 1024 * 1024


# Open a SQLite connection.
def get_users_db():
    return sqlite3.connect(USERS_DB_PATH)


def get_cars_db():
    return sqlite3.connect(CARS_DB_PATH)


def record_recent_view(user_id, car_id):
    if not user_id or not car_id:
        return
    viewed_at = datetime.utcnow().isoformat()
    with get_cars_db() as conn:
        conn.execute(
            f"""
            INSERT INTO {RECENT_VIEWS_TABLE} (user_id, car_id, viewed_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, car_id) DO UPDATE SET viewed_at = excluded.viewed_at
            """,
            (user_id, car_id, viewed_at),
        )
        conn.commit()


def get_favorite_ids(user_id):
    if not user_id:
        return set()
    with get_cars_db() as conn:
        rows = conn.execute(
            f"SELECT car_id FROM {FAVORITES_TABLE} WHERE user_id = ?",
            (user_id,),
        ).fetchall()
    return {row[0] for row in rows}


def get_current_username():
    return session.get("username") or request.args.get("username", "")


def set_last_activity():
    session["last_activity"] = datetime.utcnow().timestamp()


@app.before_request
def enforce_session_timeout():
    if request.endpoint in {"login", "register", "static"}:
        return None

    username = session.get("username")
    if not username:
        return redirect(url_for("login"))

    last_activity = session.get("last_activity")
    if last_activity is not None:
        idle_seconds = datetime.utcnow().timestamp() - float(last_activity)
        if idle_seconds > app.config["PERMANENT_SESSION_LIFETIME"].total_seconds():
            session.clear()
            flash("Session expired due to inactivity. Please log in again.")
            return redirect(url_for("login"))

    set_last_activity()
    return None


@app.context_processor
def inject_unread_count():
    username = session.get("username")
    if not username:
        return {}
    user = get_user_by_username(username)
    if not user:
        return {}
    with get_cars_db() as conn:
        count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {MESSAGES_TABLE}
            WHERE recipient_id = ? AND read_at IS NULL
            """,
            (user["id"],),
        ).fetchone()[0]
    return {"unread_count": count}


@app.context_processor
def inject_defaults():
    return {"default_car_image": DEFAULT_CAR_IMAGE}


def get_user_by_username(username):
    if not username:
        return None
    with get_users_db() as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            f"""
            SELECT id, first_name, last_name, username, email, phone, city, country, verified, is_admin
            FROM {TABLE_NAME}
            WHERE username = ?
            """,
            (username,),
        ).fetchone()


def get_user_by_id(user_id):
    if not user_id:
        return None
    with get_users_db() as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            f"""
            SELECT id, first_name, last_name, username, email, phone, city, country, verified, is_admin
            FROM {TABLE_NAME}
            WHERE id = ?
            """,
            (user_id,),
        ).fetchone()


def ensure_upload_dir():
    os.makedirs(UPLOAD_DIR, exist_ok=True)


def get_default_car_gallery():
    return [{"file_path": DEFAULT_CAR_IMAGE} for _ in range(DEFAULT_CAR_GALLERY_SIZE)]


def resolve_static_image(file_path):
    if not file_path:
        return None
    rel_path = str(file_path).replace("\\", "/")
    abs_path = os.path.join(BASE_DIR, "static", rel_path)
    return rel_path if os.path.isfile(abs_path) else None


def apply_car_image_fallback(cars):
    updated = []
    for car in cars:
        item = dict(car)
        image_path = resolve_static_image(item.get("image_path"))
        item["image_path"] = image_path or DEFAULT_CAR_IMAGE
        updated.append(item)
    return updated


def get_car_by_id(car_id):
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        car = conn.execute(
            f"SELECT * FROM {CAR_TABLE} WHERE id = ?",
            (car_id,),
        ).fetchone()
        if not car:
            return None, []
        rows = conn.execute(
            f"SELECT file_path FROM {CAR_IMAGE_TABLE} WHERE car_id = ? ORDER BY id ASC",
            (car_id,),
        ).fetchall()
        images = []
        for row in rows:
            file_path = resolve_static_image(row["file_path"])
            if file_path:
                images.append({"file_path": file_path})
        return car, images


def build_badges(car):
    badges = []
    try:
        if car["mileage"] is not None and int(car["mileage"]) <= 60000:
            badges.append("Low km")
    except Exception:
        pass
    try:
        if car["year"] is not None and int(car["year"]) >= 2021:
            badges.append("Newer model")
    except Exception:
        pass
    try:
        if car["price"] is not None and int(car["price"]) <= 10000:
            badges.append("Budget")
    except Exception:
        pass
    return badges


def get_seller_rating_map(seller_ids):
    if not seller_ids:
        return {}
    placeholders = ",".join("?" for _ in seller_ids)
    with get_cars_db() as conn:
        rows = conn.execute(
            f"""
            SELECT seller_id,
                   AVG((reliability + accuracy + communication + product) / 4.0) AS avg_rating,
                   COUNT(*) AS rating_count
            FROM {RATINGS_TABLE}
            WHERE seller_id IN ({placeholders})
            GROUP BY seller_id
            """,
            tuple(seller_ids),
        ).fetchall()
    return {row[0]: (row[1], row[2]) for row in rows}


def validate_images(images):
    allowed_exts = {".jpg", ".jpeg", ".png"}
    allowed_mimes = {"image/jpeg", "image/png"}
    max_size = app.config["MAX_IMAGE_BYTES"]
    for image in images:
        if not image or not image.filename:
            continue
        _, ext = os.path.splitext(image.filename.lower())
        if ext not in allowed_exts:
            return False, "Only JPG and PNG images are allowed."
        if image.mimetype not in allowed_mimes:
            return False, "Only JPG and PNG images are allowed."
        try:
            image.stream.seek(0, os.SEEK_END)
            size = image.stream.tell()
            image.stream.seek(0)
        except Exception:
            return False, "Could not read image size."
        if size > max_size:
            return False, "Each image must be 1MB or меньше."
    return True, ""


def build_simple_pdf(lines):
    def escape(text):
        return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")

    content_lines = []
    y = 760
    for line in lines:
        content_lines.append(f"BT /F1 12 Tf 50 {y} Td ({escape(line)}) Tj ET")
        y -= 18
    content = "\n".join(content_lines).encode("utf-8")

    objects = []
    objects.append(b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n")
    objects.append(b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n")
    objects.append(
        b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>\nendobj\n"
    )
    objects.append(
        b"4 0 obj\n<< /Length %d >>\nstream\n%s\nendstream\nendobj\n"
        % (len(content), content)
    )
    objects.append(b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n")

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf))
        pdf += obj
    xref_offset = len(pdf)
    pdf += f"xref\n0 {len(offsets)}\n".encode("utf-8")
    pdf += b"0000000000 65535 f \n"
    for off in offsets[1:]:
        pdf += f"{off:010d} 00000 n \n".encode("utf-8")
    pdf += b"trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%EOF\n" % (
        len(offsets),
        xref_offset,
    )
    return pdf


# Create the users table if it does not exist.
def init_db():
    with get_users_db() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                verified INTEGER NOT NULL DEFAULT 0,
                is_admin INTEGER NOT NULL DEFAULT 0,
                phone TEXT,
                city TEXT,
                country TEXT
            )
            """
        )
        existing_cols = {
            row[1] for row in conn.execute(f"PRAGMA table_info({TABLE_NAME})").fetchall()
        }
        for col_name, col_type in (
            ("phone", "TEXT"),
            ("city", "TEXT"),
            ("country", "TEXT"),
            ("verified", "INTEGER NOT NULL DEFAULT 0"),
            ("is_admin", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if col_name not in existing_cols:
                conn.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col_name} {col_type}")
        admin_exists = conn.execute(
            f"SELECT 1 FROM {TABLE_NAME} WHERE username = ?",
            ("admin",),
        ).fetchone()
        if not admin_exists:
            conn.execute(
                f"""
                INSERT INTO {TABLE_NAME}
                (first_name, last_name, username, email, password_hash, created_at, verified, is_admin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "Admin",
                    "User",
                    "admin",
                    "admin@example.com",
                    generate_password_hash("admin"),
                    datetime.utcnow().isoformat(),
                    1,
                    1,
                ),
            )
        conn.commit()
    with get_cars_db() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CAR_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                price INTEGER NOT NULL,
                year INTEGER NOT NULL,
                mileage INTEGER NOT NULL,
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                color TEXT NOT NULL,
                fuel TEXT NOT NULL,
                transmission TEXT NOT NULL,
                body_style TEXT NOT NULL,
                description TEXT,
                city TEXT,
                phone TEXT,
                country TEXT,
                status TEXT NOT NULL DEFAULT '{CAR_STATUS_ACTIVE}',
                created_at TEXT NOT NULL
            )
            """
        )
        car_cols = {
            row[1] for row in conn.execute(f"PRAGMA table_info({CAR_TABLE})").fetchall()
        }
        for col_name, col_type in (
            ("city", "TEXT"),
            ("phone", "TEXT"),
            ("country", "TEXT"),
            ("status", f"TEXT NOT NULL DEFAULT '{CAR_STATUS_ACTIVE}'"),
        ):
            if col_name not in car_cols:
                conn.execute(f"ALTER TABLE {CAR_TABLE} ADD COLUMN {col_name} {col_type}")
        conn.execute(
            f"""
            UPDATE {CAR_TABLE}
            SET status = ?
            WHERE status IS NULL OR status = ''
            """,
            (CAR_STATUS_ACTIVE,),
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {CAR_IMAGE_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_id INTEGER NOT NULL,
                file_path TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAVORITES_TABLE} (
                user_id INTEGER NOT NULL,
                car_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (user_id, car_id)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RECENT_VIEWS_TABLE} (
                user_id INTEGER NOT NULL,
                car_id INTEGER NOT NULL,
                viewed_at TEXT NOT NULL,
                PRIMARY KEY (user_id, car_id)
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TRANSACTIONS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_id INTEGER NOT NULL,
                seller_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                completed_at TEXT NOT NULL
            )
            """
        )
        tx_cols = {
            row[1] for row in conn.execute(f"PRAGMA table_info({TRANSACTIONS_TABLE})").fetchall()
        }
        if "status" not in tx_cols:
            conn.execute(
                f"ALTER TABLE {TRANSACTIONS_TABLE} ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'"
            )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RATINGS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id INTEGER NOT NULL UNIQUE,
                seller_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                reliability INTEGER NOT NULL,
                accuracy INTEGER NOT NULL,
                communication INTEGER NOT NULL,
                product INTEGER NOT NULL,
                comment TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        rating_cols = {
            row[1] for row in conn.execute(f"PRAGMA table_info({RATINGS_TABLE})").fetchall()
        }
        if "comment" not in rating_cols:
            conn.execute(f"ALTER TABLE {RATINGS_TABLE} ADD COLUMN comment TEXT")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {THREADS_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                car_id INTEGER NOT NULL,
                seller_id INTEGER NOT NULL,
                buyer_id INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {MESSAGES_TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER NOT NULL,
                sender_id INTEGER NOT NULL,
                recipient_id INTEGER NOT NULL,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL,
                read_at TEXT
            )
            """
        )
        conn.commit()


# Redirect root to login page.
@app.route("/")
def index():
    return redirect(url_for("login"))


# Login page and form handler.
@app.route("/login", methods=["GET", "POST"])
def login():
    # Handle form submit.
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        # Basic validation.
        if not username or not password:
            flash("Please enter both username and password.")
            return render_template("login.html", app_name=APP_NAME)

        # Fetch stored hash for this username.
        with get_users_db() as conn:
            row = conn.execute(
                f"SELECT id, username, password_hash FROM {TABLE_NAME} WHERE username = ?",
                (username,),
            ).fetchone()

        # Compare password with hash and redirect on success.
        if row and check_password_hash(row[2], password):
            session.clear()
            session.permanent = True
            session["username"] = row[1]
            set_last_activity()
            return redirect(url_for("main_page", username=row[1]))

        # Invalid credentials.
        flash("Invalid username or password.")

    return render_template("login.html", app_name=APP_NAME)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# Simple landing page after login.
@app.route("/main")
def main_page():
    username = get_current_username()
    user = get_user_by_username(username)
    favorite_ids = get_favorite_ids(user["id"]) if user else set()
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.status = ?
            ORDER BY c.created_at DESC
            LIMIT 4
            """,
            (CAR_STATUS_ACTIVE,),
        ).fetchall()
        recent_cars = []
        if user:
            recent_cars = conn.execute(
                f"""
                SELECT c.*,
                    (
                        SELECT file_path FROM {CAR_IMAGE_TABLE}
                        WHERE car_id = c.id
                        ORDER BY id ASC
                        LIMIT 1
                    ) AS image_path
                FROM {RECENT_VIEWS_TABLE} rv
                JOIN {CAR_TABLE} c ON c.id = rv.car_id
                WHERE rv.user_id = ? AND c.status = ?
                ORDER BY rv.viewed_at DESC
                LIMIT 8
                """,
                (user["id"], CAR_STATUS_ACTIVE),
            ).fetchall()
    cars = apply_car_image_fallback(cars)
    recent_cars = apply_car_image_fallback(recent_cars)
    badge_map = {car["id"]: build_badges(car) for car in cars}
    seller_ids = {car["user_id"] for car in cars} | {car["user_id"] for car in recent_cars}
    rating_map = get_seller_rating_map(seller_ids)
    for car in recent_cars:
        badge_map.setdefault(car["id"], build_badges(car))
    return render_template(
        "main.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        cars=cars,
        recent_cars=recent_cars,
        favorite_ids=favorite_ids,
        badge_map=badge_map,
        rating_map=rating_map,
    )


# Catalog page with filters and grid
@app.route("/catalog")
def catalog():
    username = get_current_username()
    user = get_user_by_username(username)
    favorite_ids = get_favorite_ids(user["id"]) if user else set()
    filters = {
        "price_min": request.args.get("price_min", "").strip(),
        "price_max": request.args.get("price_max", "").strip(),
        "year_min": request.args.get("year_min", "").strip(),
        "mileage_max": request.args.get("mileage_max", "").strip(),
        "make": request.args.get("make", "").strip(),
        "model": request.args.get("model", "").strip(),
        "color": request.args.get("color", "").strip(),
        "fuel": request.args.get("fuel", "").strip(),
        "transmission": request.args.get("transmission", "").strip(),
        "body_style": request.args.get("body_style", "").strip(),
        "city": request.args.get("city", "").strip(),
    }
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        where_clauses = ["c.status = ?"]
        params = [CAR_STATUS_ACTIVE]

        def add_range(field, min_val, max_val):
            if min_val:
                where_clauses.append(f"{field} >= ?")
                params.append(int(min_val))
            if max_val:
                where_clauses.append(f"{field} <= ?")
                params.append(int(max_val))

        def add_max(field, max_val):
            if max_val:
                where_clauses.append(f"{field} <= ?")
                params.append(int(max_val))

        def add_like(field, value):
            if value:
                where_clauses.append(f"{field} LIKE ?")
                params.append(f"%{value}%")

        add_range("c.price", filters["price_min"], filters["price_max"])
        add_range("c.year", filters["year_min"], None)
        add_max("c.mileage", filters["mileage_max"])
        add_like("c.make", filters["make"])
        add_like("c.model", filters["model"])
        add_like("c.color", filters["color"])
        add_like("c.fuel", filters["fuel"])
        add_like("c.transmission", filters["transmission"])
        add_like("c.body_style", filters["body_style"])
        add_like("c.city", filters["city"])

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            {where_sql}
            ORDER BY c.created_at DESC
            """,
            params,
        ).fetchall()
    cars = apply_car_image_fallback(cars)
    badge_map = {car["id"]: build_badges(car) for car in cars}
    seller_ids = {car["user_id"] for car in cars}
    rating_map = get_seller_rating_map(seller_ids)
    return render_template(
        "catalog.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        cars=cars,
        filters=filters,
        favorite_ids=favorite_ids,
        badge_map=badge_map,
        rating_map=rating_map,
    )


# Profile page
@app.route("/profile", methods=["GET", "POST"])
def profile():
    username = get_current_username()
    user = get_user_by_username(username)
    if request.method == "POST":
        phone = request.form.get("phone", "").strip()
        city = request.form.get("city", "").strip()
        country = request.form.get("country", "").strip()
        with get_users_db() as conn:
            conn.execute(
                f"""
                UPDATE {TABLE_NAME}
                SET phone = ?, city = ?, country = ?
                WHERE username = ?
                """,
                (phone, city, country, username),
            )
            conn.commit()
        with get_cars_db() as conn:
            conn.execute(
                f"""
                UPDATE {CAR_TABLE}
                SET phone = ?, city = ?, country = ?
                WHERE user_id = ?
                """,
                (phone, city, country, user["id"]),
            )
            conn.commit()
        return redirect(url_for("profile", username=username))

    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        user_cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.user_id = ?
            ORDER BY c.created_at DESC
            """,
            (user["id"],),
        ).fetchall()
        transactions_sold = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.buyer_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.seller_id = ?
            ORDER BY t.completed_at DESC
            """,
            (user["id"],),
        ).fetchall()
        completed_sales_count = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {TRANSACTIONS_TABLE}
            WHERE seller_id = ? AND status = ?
            """,
            (user["id"], "completed"),
        ).fetchone()[0]
        transactions_bought = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.seller_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.buyer_id = ?
            ORDER BY t.completed_at DESC
            """,
            (user["id"],),
        ).fetchall()
        ratings = conn.execute(
            f"""
            SELECT transaction_id, reliability, accuracy, communication, product, comment
            FROM {RATINGS_TABLE}
            """
        ).fetchall()
    user_cars = apply_car_image_fallback(user_cars)
    rating_map = {row[0]: row for row in ratings}
    buyer_ids = {row[2] for row in transactions_sold}
    seller_ids = {row[2] for row in transactions_bought}
    user_ids = buyer_ids | seller_ids
    profiles = {}
    if user_ids:
        placeholders = ",".join("?" for _ in user_ids)
        with get_users_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, first_name, last_name, username
                FROM {TABLE_NAME}
                WHERE id IN ({placeholders})
                """,
                tuple(user_ids),
            ).fetchall()
            profiles = {row["id"]: row for row in rows}

    return render_template(
        "profile.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        user_cars=user_cars,
        transactions_sold=transactions_sold,
        transactions_bought=transactions_bought,
        rating_map=rating_map,
        profiles=profiles,
        completed_sales_count=completed_sales_count,
    )


@app.route("/add-listing", methods=["GET", "POST"])
def add_listing():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to add a listing.")
        return redirect(url_for("login"))

    if request.method == "POST":
        ensure_upload_dir()
        price = request.form.get("price", "").strip()
        year = request.form.get("year", "").strip()
        mileage = request.form.get("mileage", "").strip()
        make = request.form.get("make", "").strip()
        model = request.form.get("model", "").strip()
        color = request.form.get("color", "").strip()
        fuel = request.form.get("fuel", "").strip()
        transmission = request.form.get("transmission", "").strip()
        body_style = request.form.get("body_style", "").strip()
        description = request.form.get("description", "").strip()
        city = (user["city"] or "").strip()
        phone = (user["phone"] or "").strip()
        country = (user["country"] or "").strip()

        required_fields = {
            "price": price,
            "year": year,
            "mileage": mileage,
            "make": make,
            "model": model,
            "color": color,
            "fuel": fuel,
            "transmission": transmission,
            "body_style": body_style,
            "description": description,
        }
        missing = [label for label, value in required_fields.items() if not value]
        if missing:
            flash("Missing fields: " + ", ".join(missing))
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )
        if make not in ALLOWED_MAKES or model not in ALLOWED_MAKES.get(make, []):
            flash("Please select a valid make and model.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )
        if color not in ALLOWED_COLORS:
            flash("Please select a valid color.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )
        if fuel not in ALLOWED_FUELS:
            flash("Please select a valid fuel type.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )
        if transmission not in ALLOWED_TRANSMISSIONS:
            flash("Please select a valid transmission.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )
        if body_style not in ALLOWED_BODY_STYLES:
            flash("Please select a valid body style.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )

        created_at = datetime.utcnow().isoformat()
        if not (str(year).isdigit() and MIN_YEAR <= int(year) <= MAX_YEAR):
            flash(f"Year must be between {MIN_YEAR} and {MAX_YEAR}.")
            return render_template(
                "add_listing.html", app_name=APP_NAME, username=username, user=user
            )

        with get_cars_db() as conn:
            cursor = conn.execute(
                f"""
                INSERT INTO {CAR_TABLE}
                (user_id, price, year, mileage, make, model, color, fuel, transmission,
                 body_style, description, city, phone, country, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user["id"],
                    int(price),
                    int(year),
                    int(mileage),
                    make,
                    model,
                    color,
                    fuel,
                    transmission,
                    body_style,
                    description,
                    city,
                    phone,
                    country,
                    CAR_STATUS_ACTIVE,
                    created_at,
                ),
            )
            car_id = cursor.lastrowid

            images = request.files.getlist("images")
            images = [img for img in images if img and img.filename]
            ok, msg = validate_images(images)
            if not ok:
                flash(msg)
                return render_template(
                    "add_listing.html", app_name=APP_NAME, username=username, user=user
                )
            if len(images) > 15:
                flash("You can upload up to 15 photos.")
                return render_template(
                    "add_listing.html", app_name=APP_NAME, username=username, user=user
                )
            images = images[:15]
            for image in images:
                if not image or not image.filename:
                    continue
                safe_name = secure_filename(image.filename)
                unique_name = f"{uuid.uuid4().hex}_{safe_name}"
                save_path = os.path.join(UPLOAD_DIR, unique_name)
                image.save(save_path)
                rel_path = f"uploads/{unique_name}"
                conn.execute(
                    f"INSERT INTO {CAR_IMAGE_TABLE} (car_id, file_path) VALUES (?, ?)",
                    (car_id, rel_path),
                )
            conn.commit()

        return redirect(url_for("my_listings", username=username))

    return render_template("add_listing.html", app_name=APP_NAME, username=username, user=user)


@app.route("/my-listings")
def my_listings():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to view your listings.")
        return redirect(url_for("login"))

    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.user_id = ? AND c.status = ?
            ORDER BY c.created_at DESC
            """,
            (user["id"], CAR_STATUS_ACTIVE),
        ).fetchall()
        completed_cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.user_id = ? AND c.status = ?
            ORDER BY c.created_at DESC
            """,
            (user["id"], CAR_STATUS_COMPLETED),
        ).fetchall()
        transactions_sold = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.buyer_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.seller_id = ?
            ORDER BY t.completed_at DESC
            """,
            (user["id"],),
        ).fetchall()
        transactions_bought = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.seller_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.buyer_id = ?
            ORDER BY t.completed_at DESC
            """,
            (user["id"],),
        ).fetchall()
        ratings = conn.execute(
            f"""
            SELECT transaction_id, reliability, accuracy, communication, product, comment
            FROM {RATINGS_TABLE}
            """
        ).fetchall()
    cars = apply_car_image_fallback(cars)
    completed_cars = apply_car_image_fallback(completed_cars)
    badge_map = {car["id"]: build_badges(car) for car in cars}
    for car in completed_cars:
        badge_map.setdefault(car["id"], build_badges(car))
    seller_ids = {car["user_id"] for car in cars} | {car["user_id"] for car in completed_cars}
    rating_map = get_seller_rating_map(seller_ids)
    rating_tx_map = {row[0]: row for row in ratings}
    buyer_ids = {row[2] for row in transactions_sold}
    seller_ids = {row[2] for row in transactions_bought}
    user_ids = buyer_ids | seller_ids
    profiles = {}
    if user_ids:
        placeholders = ",".join("?" for _ in user_ids)
        with get_users_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, first_name, last_name, username
                FROM {TABLE_NAME}
                WHERE id IN ({placeholders})
                """,
                tuple(user_ids),
            ).fetchall()
            profiles = {row["id"]: row for row in rows}

    return render_template(
        "my_listings.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        cars=cars,
        completed_cars=completed_cars,
        badge_map=badge_map,
        rating_map=rating_map,
        transactions_sold=transactions_sold,
        transactions_bought=transactions_bought,
        rating_tx_map=rating_tx_map,
        profiles=profiles,
    )


@app.route("/car/<int:car_id>")
def car_details(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    car, images = get_car_by_id(car_id)
    if not images:
        images = get_default_car_gallery()
    if not car:
        flash("Listing not found.")
        return redirect(url_for("catalog", username=username))
    is_owner = bool(user) and car["user_id"] == user["id"]
    is_favorite = bool(user) and car["id"] in get_favorite_ids(user["id"])
    if user:
        record_recent_view(user["id"], car["id"])
    seller = get_user_by_id(car["user_id"])
    rating_map = get_seller_rating_map({car["user_id"]})
    seller_rating = rating_map.get(car["user_id"])
    buyers = []
    with get_users_db() as conn:
        conn.row_factory = sqlite3.Row
        buyers = conn.execute(
            f"""
            SELECT id, first_name, last_name, username
            FROM {TABLE_NAME}
            WHERE id != ?
            ORDER BY first_name, last_name
            """,
            (car["user_id"],),
        ).fetchall()
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        similar_cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.id != ? AND c.status = ? AND c.make = ? AND c.model = ?
            ORDER BY c.created_at DESC
            LIMIT 4
            """,
            (car["id"], CAR_STATUS_ACTIVE, car["make"], car["model"]),
        ).fetchall()
    similar_cars = apply_car_image_fallback(similar_cars)
    return render_template(
        "car_details.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        seller=seller,
        car=car,
        images=images,
        is_owner=is_owner,
        is_favorite=is_favorite,
        badges=build_badges(car),
        buyers=buyers,
        seller_rating=seller_rating,
        similar_cars=similar_cars,
    )


@app.route("/car/<int:car_id>/edit", methods=["GET", "POST"])
def edit_car(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to edit listings.")
        return redirect(url_for("login"))

    car, images = get_car_by_id(car_id)
    if not images:
        images = get_default_car_gallery()
    if not car:
        flash("Listing not found.")
        return redirect(url_for("catalog", username=username))
    if car["user_id"] != user["id"]:
        flash("You can only edit your own listings.")
        return redirect(url_for("car_details", car_id=car_id, username=username))
    if car["status"] != CAR_STATUS_ACTIVE:
        flash("Completed listings cannot be edited.")
        return redirect(url_for("car_details", car_id=car_id, username=username))

    if request.method == "POST":
        ensure_upload_dir()
        price = request.form.get("price", "").strip()
        year = request.form.get("year", "").strip()
        mileage = request.form.get("mileage", "").strip()
        make = request.form.get("make", "").strip()
        model = request.form.get("model", "").strip()
        color = request.form.get("color", "").strip()
        fuel = request.form.get("fuel", "").strip()
        transmission = request.form.get("transmission", "").strip()
        body_style = request.form.get("body_style", "").strip()
        description = request.form.get("description", "").strip()

        required_fields = {
            "price": price,
            "year": year,
            "mileage": mileage,
            "make": make,
            "model": model,
            "color": color,
            "fuel": fuel,
            "transmission": transmission,
            "body_style": body_style,
            "description": description,
        }
        missing = [label for label, value in required_fields.items() if not value]
        if missing:
            flash("Missing fields: " + ", ".join(missing))
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if make not in ALLOWED_MAKES or model not in ALLOWED_MAKES.get(make, []):
            flash("Please select a valid make and model.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if color not in ALLOWED_COLORS:
            flash("Please select a valid color.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if fuel not in ALLOWED_FUELS:
            flash("Please select a valid fuel type.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if transmission not in ALLOWED_TRANSMISSIONS:
            flash("Please select a valid transmission.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if body_style not in ALLOWED_BODY_STYLES:
            flash("Please select a valid body style.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )
        if not (str(year).isdigit() and MIN_YEAR <= int(year) <= MAX_YEAR):
            flash(f"Year must be between {MIN_YEAR} and {MAX_YEAR}.")
            return render_template(
                "edit_listing.html",
                app_name=APP_NAME,
                username=username,
                user=user,
                car=car,
                images=images,
            )

        with get_cars_db() as conn:
            conn.execute(
                f"""
                UPDATE {CAR_TABLE}
                SET price = ?, year = ?, mileage = ?, make = ?, model = ?, color = ?,
                    fuel = ?, transmission = ?, body_style = ?, description = ?
                WHERE id = ?
                """,
                (
                    int(price),
                    int(year),
                    int(mileage),
                    make,
                    model,
                    color,
                    fuel,
                    transmission,
                    body_style,
                    description,
                    car_id,
                ),
            )

            images_upload = request.files.getlist("images")
            images_upload = [img for img in images_upload if img and img.filename]
            if images_upload:
                ok, msg = validate_images(images_upload)
                if not ok:
                    flash(msg)
                    return render_template(
                        "edit_listing.html",
                        app_name=APP_NAME,
                        username=username,
                        user=user,
                        car=car,
                        images=images,
                    )
                existing_count = conn.execute(
                    f"SELECT COUNT(*) FROM {CAR_IMAGE_TABLE} WHERE car_id = ?",
                    (car_id,),
                ).fetchone()[0]
                if existing_count + len(images_upload) > 15:
                    flash("You can upload up to 15 photos total.")
                    conn.rollback()
                    return render_template(
                        "edit_listing.html",
                        app_name=APP_NAME,
                        username=username,
                        user=user,
                        car=car,
                        images=images,
                    )
                for image in images_upload:
                    safe_name = secure_filename(image.filename)
                    unique_name = f"{uuid.uuid4().hex}_{safe_name}"
                    save_path = os.path.join(UPLOAD_DIR, unique_name)
                    image.save(save_path)
                    rel_path = f"uploads/{unique_name}"
                    conn.execute(
                        f"INSERT INTO {CAR_IMAGE_TABLE} (car_id, file_path) VALUES (?, ?)",
                        (car_id, rel_path),
                    )
            conn.commit()

        return redirect(url_for("car_details", car_id=car_id, username=username))

    return render_template(
        "edit_listing.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        car=car,
        images=images,
    )


@app.route("/car/<int:car_id>/complete", methods=["POST"])
def complete_car(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to manage listings.")
        return redirect(url_for("login"))

    car, _ = get_car_by_id(car_id)
    if not car:
        flash("Listing not found.")
        return redirect(url_for("my_listings", username=username))
    if car["user_id"] != user["id"]:
        flash("You can only update your own listings.")
        return redirect(url_for("car_details", car_id=car_id, username=username))

    buyer_id = request.form.get("buyer_id", "").strip()
    if not buyer_id.isdigit() or int(buyer_id) == user["id"]:
        flash("Please select a valid buyer.")
        return redirect(url_for("car_details", car_id=car_id, username=username))

    buyer = get_user_by_id(int(buyer_id))
    if not buyer:
        flash("Buyer not found.")
        return redirect(url_for("car_details", car_id=car_id, username=username))

    with get_cars_db() as conn:
        conn.execute(
            f"UPDATE {CAR_TABLE} SET status = ? WHERE id = ?",
            (CAR_STATUS_COMPLETED, car_id),
        )
        conn.execute(
            f"""
            INSERT INTO {TRANSACTIONS_TABLE}
            (car_id, seller_id, buyer_id, status, completed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (car_id, user["id"], int(buyer_id), "pending", datetime.utcnow().isoformat()),
        )
        conn.commit()

    return redirect(url_for("my_listings", username=username))


@app.route("/car/<int:car_id>/delete", methods=["POST"])
def remove_car_record(car_id):
    car, images = get_car_by_id(car_id)
    if not images:
        images = get_default_car_gallery()
    if not car:
        return False
    with get_cars_db() as conn:
        tx_ids = conn.execute(
            f"SELECT id FROM {TRANSACTIONS_TABLE} WHERE car_id = ?",
            (car_id,),
        ).fetchall()
        if tx_ids:
            placeholders = ",".join("?" for _ in tx_ids)
            conn.execute(
                f"DELETE FROM {RATINGS_TABLE} WHERE transaction_id IN ({placeholders})",
                tuple(row[0] for row in tx_ids),
            )
        conn.execute(
            f"DELETE FROM {TRANSACTIONS_TABLE} WHERE car_id = ?",
            (car_id,),
        )
        conn.execute(f"DELETE FROM {CAR_IMAGE_TABLE} WHERE car_id = ?", (car_id,))
        conn.execute(f"DELETE FROM {CAR_TABLE} WHERE id = ?", (car_id,))
        conn.commit()
    for img in images:
        rel_path = img["file_path"]
        if rel_path:
            file_path = os.path.join("static", rel_path)
            if os.path.exists(file_path):
                os.remove(file_path)
    return True


@app.route("/car/<int:car_id>/delete", methods=["POST"])
def delete_car(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to manage listings.")
        return redirect(url_for("login"))

    car, _ = get_car_by_id(car_id)
    if not car:
        flash("Listing not found.")
        return redirect(url_for("my_listings", username=username))
    if car["user_id"] != user["id"]:
        flash("You can only delete your own listings.")
        return redirect(url_for("car_details", car_id=car_id, username=username))

    remove_car_record(car_id)
    return redirect(url_for("my_listings", username=username))


def get_or_create_thread(car_id, seller_id, buyer_id):
    with get_cars_db() as conn:
        row = conn.execute(
            f"""
            SELECT id FROM {THREADS_TABLE}
            WHERE car_id = ? AND seller_id = ? AND buyer_id = ?
            """,
            (car_id, seller_id, buyer_id),
        ).fetchone()
        if row:
            return row[0]
        cur = conn.execute(
            f"""
            INSERT INTO {THREADS_TABLE} (car_id, seller_id, buyer_id, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (car_id, seller_id, buyer_id, datetime.utcnow().isoformat()),
        )
        conn.commit()
        return cur.lastrowid


@app.route("/messages")
def messages():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        threads = conn.execute(
            f"""
            SELECT t.*,
                (
                    SELECT body FROM {MESSAGES_TABLE}
                    WHERE thread_id = t.id
                    ORDER BY created_at DESC
                    LIMIT 1
                ) AS last_message,
                (
                    SELECT COUNT(*) FROM {MESSAGES_TABLE}
                    WHERE thread_id = t.id AND recipient_id = ? AND read_at IS NULL
                ) AS unread_count
            FROM {THREADS_TABLE} t
            WHERE t.seller_id = ? OR t.buyer_id = ?
            ORDER BY t.created_at DESC
            """,
            (user["id"], user["id"], user["id"]),
        ).fetchall()
    user_ids = set()
    car_ids = set()
    for t in threads:
        user_ids.add(t["seller_id"])
        user_ids.add(t["buyer_id"])
        car_ids.add(t["car_id"])
    profiles = {}
    cars = {}
    if user_ids:
        placeholders = ",".join("?" for _ in user_ids)
        with get_users_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT id, first_name, last_name FROM {TABLE_NAME} WHERE id IN ({placeholders})",
                tuple(user_ids),
            ).fetchall()
            profiles = {row["id"]: row for row in rows}
    if car_ids:
        placeholders = ",".join("?" for _ in car_ids)
        with get_cars_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"SELECT id, make, model, year FROM {CAR_TABLE} WHERE id IN ({placeholders})",
                tuple(car_ids),
            ).fetchall()
            cars = {row["id"]: row for row in rows}
    return render_template(
        "messages.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        threads=threads,
        profiles=profiles,
        cars=cars,
    )


@app.route("/messages/<int:thread_id>", methods=["GET", "POST"])
def message_thread(thread_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        thread = conn.execute(
            f"SELECT * FROM {THREADS_TABLE} WHERE id = ?",
            (thread_id,),
        ).fetchone()
        if not thread or (thread["seller_id"] != user["id"] and thread["buyer_id"] != user["id"]):
            flash("Thread not found.")
            return redirect(url_for("messages"))

        if request.method == "POST":
            body = request.form.get("body", "").strip()
            if body:
                recipient_id = (
                    thread["buyer_id"] if thread["seller_id"] == user["id"] else thread["seller_id"]
                )
                conn.execute(
                    f"""
                    INSERT INTO {MESSAGES_TABLE}
                    (thread_id, sender_id, recipient_id, body, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (thread_id, user["id"], recipient_id, body, datetime.utcnow().isoformat()),
                )
                conn.commit()
            return redirect(url_for("message_thread", thread_id=thread_id))

        conn.execute(
            f"""
            UPDATE {MESSAGES_TABLE}
            SET read_at = ?
            WHERE thread_id = ? AND recipient_id = ? AND read_at IS NULL
            """,
            (datetime.utcnow().isoformat(), thread_id, user["id"]),
        )
        conn.commit()
        messages_rows = conn.execute(
            f"""
            SELECT * FROM {MESSAGES_TABLE}
            WHERE thread_id = ?
            ORDER BY created_at ASC
            """,
            (thread_id,),
        ).fetchall()
    seller = get_user_by_id(thread["seller_id"])
    buyer = get_user_by_id(thread["buyer_id"])
    return render_template(
        "message_thread.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        thread=thread,
        messages=messages_rows,
        seller=seller,
        buyer=buyer,
    )


@app.route("/messages/new")
def new_message():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))
    car_id = request.args.get("car_id", "").strip()
    if not car_id.isdigit():
        return redirect(url_for("catalog", username=username))
    car, _ = get_car_by_id(int(car_id))
    if not car:
        return redirect(url_for("catalog", username=username))
    if car["user_id"] == user["id"]:
        flash("You cannot message yourself.")
        return redirect(url_for("car_details", car_id=car["id"], username=username))
    thread_id = get_or_create_thread(car["id"], car["user_id"], user["id"])
    return redirect(url_for("message_thread", thread_id=thread_id))


@app.route("/car/<int:car_id>/pdf")
def car_pdf(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))
    car, _ = get_car_by_id(car_id)
    if not car:
        flash("Listing not found.")
        return redirect(url_for("catalog", username=username))
    seller = get_user_by_id(car["user_id"])
    lines = [
        f"Listing #{car['id']}",
        f"Make: {car['make']}",
        f"Model: {car['model']}",
        f"Year: {car['year']}",
        f"Price: €{car['price']}",
        f"Mileage: {car['mileage']} km",
        f"Fuel: {car['fuel']}",
        f"Transmission: {car['transmission']}",
        f"Body: {car['body_style']}",
        f"City: {car['city'] or '-'}",
        f"Phone: {car['phone'] or '-'}",
        f"Seller: {seller['first_name']} {seller['last_name']}" if seller else "Seller: -",
        f"Description: {car['description'] or '-'}",
    ]
    pdf_bytes = build_simple_pdf(lines)
    filename = f"listing_{car_id}.pdf"
    return (
        pdf_bytes,
        200,
        {
            "Content-Type": "application/pdf",
            "Content-Disposition": f"attachment; filename={filename}",
        },
    )




@app.route("/car/<int:car_id>/favorite", methods=["POST"])
def toggle_favorite(car_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to manage favorites.")
        return redirect(url_for("login"))

    car, _ = get_car_by_id(car_id)
    if not car or car["status"] != CAR_STATUS_ACTIVE:
        flash("Listing not found.")
        return redirect(url_for("catalog", username=username))

    with get_cars_db() as conn:
        exists = conn.execute(
            f"SELECT 1 FROM {FAVORITES_TABLE} WHERE user_id = ? AND car_id = ?",
            (user["id"], car_id),
        ).fetchone()
        if exists:
            conn.execute(
                f"DELETE FROM {FAVORITES_TABLE} WHERE user_id = ? AND car_id = ?",
                (user["id"], car_id),
            )
        else:
            conn.execute(
                f"""
                INSERT INTO {FAVORITES_TABLE} (user_id, car_id, created_at)
                VALUES (?, ?, ?)
                """,
                (user["id"], car_id, datetime.utcnow().isoformat()),
            )
        conn.commit()

    next_url = request.form.get("next", "")
    if next_url.startswith("/"):
        return redirect(next_url)
    return redirect(url_for("catalog", username=username))


@app.route("/favorites")
def favorites():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))
    favorite_ids = get_favorite_ids(user["id"])
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            JOIN {FAVORITES_TABLE} f ON f.car_id = c.id
            WHERE f.user_id = ? AND c.status = ?
            ORDER BY f.created_at DESC
            """,
            (user["id"], CAR_STATUS_ACTIVE),
        ).fetchall()
    cars = apply_car_image_fallback(cars)
    badge_map = {car["id"]: build_badges(car) for car in cars}
    seller_ids = {car["user_id"] for car in cars}
    rating_map = get_seller_rating_map(seller_ids)
    return render_template(
        "favorites.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        cars=cars,
        favorite_ids=favorite_ids,
        badge_map=badge_map,
        rating_map=rating_map,
    )


@app.route("/seller/<int:seller_id>")
def seller_profile(seller_id):
    username = get_current_username()
    user = get_user_by_username(username)
    seller = get_user_by_id(seller_id)
    if not seller:
        flash("Seller not found.")
        return redirect(url_for("catalog", username=username))

    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.user_id = ? AND c.status = ?
            ORDER BY c.created_at DESC
            """,
            (seller_id, CAR_STATUS_ACTIVE),
        ).fetchall()
        transactions_sold = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.buyer_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.seller_id = ?
            ORDER BY t.completed_at DESC
            """,
            (seller_id,),
        ).fetchall()
        transactions_bought = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.seller_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.buyer_id = ?
            ORDER BY t.completed_at DESC
            """,
            (seller_id,),
        ).fetchall()
        ratings = conn.execute(
            f"""
            SELECT transaction_id, reliability, accuracy, communication, product, comment
            FROM {RATINGS_TABLE}
            """
        ).fetchall()
    cars = apply_car_image_fallback(cars)
    badge_map = {car["id"]: build_badges(car) for car in cars}
    rating_map = get_seller_rating_map({seller_id})
    favorite_ids = get_favorite_ids(user["id"]) if user else set()
    rating_tx_map = {row[0]: row for row in ratings}
    buyer_ids = {row[2] for row in transactions_sold}
    seller_ids = {row[2] for row in transactions_bought}
    user_ids = buyer_ids | seller_ids
    profiles = {}
    if user_ids:
        placeholders = ",".join("?" for _ in user_ids)
        with get_users_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, first_name, last_name, username
                FROM {TABLE_NAME}
                WHERE id IN ({placeholders})
                """,
                tuple(user_ids),
            ).fetchall()
            profiles = {row["id"]: row for row in rows}
    return render_template(
        "seller_profile.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        seller=seller,
        cars=cars,
        badge_map=badge_map,
        favorite_ids=favorite_ids,
        rating_map=rating_map,
        transactions_sold=transactions_sold,
        transactions_bought=transactions_bought,
        rating_tx_map=rating_tx_map,
        profiles=profiles,
    )


@app.route("/compare")
def compare():
    username = get_current_username()
    user = get_user_by_username(username)
    ids_raw = request.args.get("ids", "")
    ids = [part for part in ids_raw.split(",") if part.strip().isdigit()]
    ids = ids[:2]
    if not ids:
        flash("Select cars to compare first.")
        return redirect(url_for("catalog", username=username))

    placeholders = ",".join("?" for _ in ids)
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.*,
                (
                    SELECT file_path FROM {CAR_IMAGE_TABLE}
                    WHERE car_id = c.id
                    ORDER BY id ASC
                    LIMIT 1
                ) AS image_path
            FROM {CAR_TABLE} c
            WHERE c.id IN ({placeholders}) AND c.status = ?
            """,
            (*ids, CAR_STATUS_ACTIVE),
        ).fetchall()
    cars = apply_car_image_fallback(cars)
    return render_template(
        "compare.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        cars=cars,
    )


@app.route("/buyer/<int:buyer_id>")
def buyer_profile(buyer_id):
    username = get_current_username()
    user = get_user_by_username(username)
    buyer = get_user_by_id(buyer_id)
    if not buyer:
        flash("Buyer not found.")
        return redirect(url_for("catalog", username=username))

    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        purchases = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.seller_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.buyer_id = ?
            ORDER BY t.completed_at DESC
            """,
            (buyer_id,),
        ).fetchall()
        sales = conn.execute(
            f"""
            SELECT t.id, t.car_id, t.buyer_id, t.completed_at, t.status,
                   c.make, c.model, c.year, c.price
            FROM {TRANSACTIONS_TABLE} t
            JOIN {CAR_TABLE} c ON c.id = t.car_id
            WHERE t.seller_id = ?
            ORDER BY t.completed_at DESC
            """,
            (buyer_id,),
        ).fetchall()
        ratings = conn.execute(
            f"""
            SELECT transaction_id, reliability, accuracy, communication, product, comment
            FROM {RATINGS_TABLE}
            """
        ).fetchall()
    seller_ids = {row["seller_id"] for row in purchases}
    buyer_ids = {row["buyer_id"] for row in sales}
    user_ids = seller_ids | buyer_ids
    profiles = {}
    if user_ids:
        placeholders = ",".join("?" for _ in user_ids)
        with get_users_db() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"""
                SELECT id, first_name, last_name
                FROM {TABLE_NAME}
                WHERE id IN ({placeholders})
                """,
                tuple(user_ids),
            ).fetchall()
            profiles = {row["id"]: row for row in rows}
    rating_tx_map = {row[0]: row for row in ratings}
    return render_template(
        "buyer_profile.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        buyer=buyer,
        purchases=purchases,
        sales=sales,
        profiles=profiles,
        rating_tx_map=rating_tx_map,
    )


def admin_required(user):
    return bool(user) and bool(user["is_admin"])


@app.route("/admin")
def admin_panel():
    username = get_current_username()
    user = get_user_by_username(username)
    if not admin_required(user):
        flash("Admin access required.")
        return redirect(url_for("main_page", username=username))
    with get_users_db() as conn:
        conn.row_factory = sqlite3.Row
        users = conn.execute(
            f"SELECT id, first_name, last_name, username, email, verified, is_admin FROM {TABLE_NAME} ORDER BY id"
        ).fetchall()
    with get_cars_db() as conn:
        conn.row_factory = sqlite3.Row
        cars = conn.execute(
            f"""
            SELECT c.id, c.make, c.model, c.year, c.price, c.user_id
            FROM {CAR_TABLE} c
            ORDER BY c.id DESC
            """
        ).fetchall()
        ratings = conn.execute(
            f"""
            SELECT id, transaction_id, seller_id, buyer_id, reliability, accuracy, communication, product, comment
            FROM {RATINGS_TABLE}
            ORDER BY id DESC
            """
        ).fetchall()
    return render_template(
        "admin.html",
        app_name=APP_NAME,
        username=username,
        user=user,
        users=users,
        cars=cars,
        ratings=ratings,
    )


@app.route("/admin/verify", methods=["POST"])
def admin_verify_user():
    username = get_current_username()
    user = get_user_by_username(username)
    if not admin_required(user):
        return redirect(url_for("main_page", username=username))
    user_id = request.form.get("user_id", "").strip()
    value = request.form.get("value", "0").strip()
    if not user_id.isdigit():
        return redirect(url_for("admin_panel"))
    with get_users_db() as conn:
        conn.execute(
            f"UPDATE {TABLE_NAME} SET verified = ? WHERE id = ?",
            (1 if value == "1" else 0, int(user_id)),
        )
        conn.commit()
    return redirect(url_for("admin_panel"))


@app.route("/admin/delete-car", methods=["POST"])
def admin_delete_car():
    username = get_current_username()
    user = get_user_by_username(username)
    if not admin_required(user):
        return redirect(url_for("main_page", username=username))
    car_id = request.form.get("car_id", "").strip()
    if not car_id.isdigit():
        return redirect(url_for("admin_panel"))
    remove_car_record(int(car_id))
    return redirect(url_for("admin_panel"))


@app.route("/admin/delete-rating", methods=["POST"])
def admin_delete_rating():
    username = get_current_username()
    user = get_user_by_username(username)
    if not admin_required(user):
        return redirect(url_for("main_page", username=username))
    rating_id = request.form.get("rating_id", "").strip()
    if not rating_id.isdigit():
        return redirect(url_for("admin_panel"))
    with get_cars_db() as conn:
        conn.execute(f"DELETE FROM {RATINGS_TABLE} WHERE id = ?", (int(rating_id),))
        conn.commit()
    return redirect(url_for("admin_panel"))


@app.route("/verify", methods=["POST"])
def verify_seller():
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))

    agree = request.form.get("agree", "")
    if agree != "yes":
        flash("Please confirm the verification terms.")
        return redirect(url_for("profile", username=username))

    with get_cars_db() as conn:
        completed_sales = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {TRANSACTIONS_TABLE}
            WHERE seller_id = ? AND status = ?
            """,
            (user["id"], "completed"),
        ).fetchone()[0]
    if completed_sales < 5:
        flash("You need at least 5 completed transactions to verify.")
        return redirect(url_for("profile", username=username))

    with get_users_db() as conn:
        conn.execute(
            f"UPDATE {TABLE_NAME} SET verified = 1 WHERE id = ?",
            (user["id"],),
        )
        conn.commit()

    flash("Your seller profile is now verified.")
    return redirect(url_for("profile", username=username))


@app.route("/transaction/<int:transaction_id>/rate", methods=["POST"])
def rate_transaction(transaction_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        flash("Please log in to leave a rating.")
        return redirect(url_for("login"))

    reliability = request.form.get("reliability", "").strip()
    accuracy = request.form.get("accuracy", "").strip()
    communication = request.form.get("communication", "").strip()
    product = request.form.get("product", "").strip()
    comment = request.form.get("comment", "").strip()

    scores = [reliability, accuracy, communication, product]
    if not all(score.isdigit() and 1 <= int(score) <= 5 for score in scores):
        flash("All ratings must be between 1 and 5.")
        return redirect(url_for("profile", username=username))

    with get_cars_db() as conn:
        tx = conn.execute(
            f"""
            SELECT id, seller_id, buyer_id, status
            FROM {TRANSACTIONS_TABLE}
            WHERE id = ?
            """,
            (transaction_id,),
        ).fetchone()
        if not tx or tx[2] != user["id"] or tx[3] != "completed":
            flash("You can only rate your own completed purchases.")
            return redirect(url_for("profile", username=username))

        conn.execute(
            f"""
            INSERT INTO {RATINGS_TABLE}
            (transaction_id, seller_id, buyer_id, reliability, accuracy, communication, product, comment, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(transaction_id) DO UPDATE SET
                reliability = excluded.reliability,
                accuracy = excluded.accuracy,
                communication = excluded.communication,
                product = excluded.product,
                comment = excluded.comment
            """,
            (
                transaction_id,
                tx[1],
                tx[2],
                int(reliability),
                int(accuracy),
                int(communication),
                int(product),
                comment,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()

    flash("Thanks for your rating!")
    return redirect(url_for("profile", username=username))


@app.route("/transaction/<int:transaction_id>/confirm", methods=["POST"])
def confirm_transaction(transaction_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))

    with get_cars_db() as conn:
        tx = conn.execute(
            f"""
            SELECT id, buyer_id, status
            FROM {TRANSACTIONS_TABLE}
            WHERE id = ?
            """,
            (transaction_id,),
        ).fetchone()
        if not tx or tx[1] != user["id"]:
            flash("You can only confirm your own purchase.")
            return redirect(url_for("profile", username=username))
        conn.execute(
            f"UPDATE {TRANSACTIONS_TABLE} SET status = ? WHERE id = ?",
            ("completed", transaction_id),
        )
        conn.commit()

    return redirect(url_for("profile", username=username))


@app.route("/transaction/<int:transaction_id>/cancel", methods=["POST"])
def cancel_transaction(transaction_id):
    username = get_current_username()
    user = get_user_by_username(username)
    if not user:
        return redirect(url_for("login"))

    with get_cars_db() as conn:
        tx = conn.execute(
            f"""
            SELECT id, buyer_id, seller_id, car_id, status
            FROM {TRANSACTIONS_TABLE}
            WHERE id = ?
            """,
            (transaction_id,),
        ).fetchone()
        if not tx or (tx[1] != user["id"] and tx[2] != user["id"]):
            flash("You can only cancel your own transaction.")
            return redirect(url_for("profile", username=username))
        conn.execute(
            f"UPDATE {TRANSACTIONS_TABLE} SET status = ? WHERE id = ?",
            ("canceled", transaction_id),
        )
        conn.execute(
            f"UPDATE {CAR_TABLE} SET status = ? WHERE id = ?",
            (CAR_STATUS_ACTIVE, tx[3]),
        )
        conn.commit()

    return redirect(url_for("profile", username=username))

# Registration page and form handler.
@app.route("/register", methods=["GET", "POST"])
def register():
    # Handle form submit.
    if request.method == "POST":
        first_name = request.form.get("first_name", "").strip()
        last_name = request.form.get("last_name", "").strip()
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        confirm_password = request.form.get("confirm_password", "")

        # Required fields validation.
        if not all([first_name, last_name, username, email, password, confirm_password]):
            flash("All fields are required.")
            return render_template("register.html", app_name=APP_NAME)

        # Password match check.
        if password != confirm_password:
            flash("Passwords do not match.")
            return render_template("register.html", app_name=APP_NAME)

        # Minimal password length check.
        if len(password) < 8:
            flash("Password must be at least 8 characters.")
            return render_template("register.html", app_name=APP_NAME)

        # Hash password for secure storage.
        password_hash = generate_password_hash(password)
        created_at = datetime.utcnow().isoformat()

        # Insert user data into database.
        try:
            with get_users_db() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {TABLE_NAME} (first_name, last_name, username, email, password_hash, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (first_name, last_name, username, email, password_hash, created_at),
                )
                conn.commit()
            flash("Registration successful. You can now log in.")
            return redirect(url_for("login"))
        except sqlite3.IntegrityError:
            # Username or email already exists.
            flash("Username or email already exists.")

    return render_template("register.html", app_name=APP_NAME)


if __name__ == "__main__":
    # Ensure DB/table exists, then run dev server.
    init_db()
    app.run(debug=True)
