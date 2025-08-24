import psycopg2
import sys
import logging
from config import DB_CONFIG

# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------- CREATE DATABASE ----------------
def create_database():
    """Create the database if it doesn't exist."""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"]
        )
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}'"
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']}")
            logger.info(f"✓ Database {DB_CONFIG['database']} created")
        else:
            logger.info(f"✓ Database {DB_CONFIG['database']} already exists")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating database: {e}")
        return False


# ---------------- CREATE BRONZE ----------------
def create_bronze_schema():
    """Create Bronze schema and tables (raw, no FKs/uniques)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("✓ Bronze schema created/verified")

        # Drivers (raw)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.drivers (
                driver_id VARCHAR PRIMARY KEY,
                driver_name VARCHAR,
                email VARCHAR,
                dob DATE,
                signup_date DATE,
                driver_rating NUMERIC(3,2),
                city VARCHAR,
                license_number VARCHAR,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Vehicles (raw)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.vehicles (
                vehicle_id VARCHAR PRIMARY KEY,
                driver_id VARCHAR,
                make VARCHAR,
                model VARCHAR,
                year INT,
                plate VARCHAR,
                capacity INT,
                color VARCHAR,
                registration_date DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Riders (raw)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.riders (
                rider_id VARCHAR PRIMARY KEY,
                rider_name VARCHAR,
                email VARCHAR,
                signup_date DATE,
                home_city VARCHAR,
                rider_rating NUMERIC(3,2),
                default_payment_method VARCHAR,
                is_verified BOOLEAN,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Trips (raw)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.trips (
                trip_id VARCHAR PRIMARY KEY,
                rider_id VARCHAR,
                driver_id VARCHAR,
                vehicle_id VARCHAR,
                request_ts TIMESTAMP,
                pickup_ts TIMESTAMP,
                dropoff_ts TIMESTAMP,
                pickup_location VARCHAR,
                drop_location VARCHAR,
                distance_km NUMERIC(6,2),
                duration_min NUMERIC(6,2),
                wait_time_minutes NUMERIC(5,2),
                surge_multiplier NUMERIC(3,2),
                base_fare_usd NUMERIC(8,2),
                tax_usd NUMERIC(8,2),
                tip_usd NUMERIC(8,2),
                total_fare_usd NUMERIC(10,2),
                status VARCHAR,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Payments (raw)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.payments (
                payment_id VARCHAR PRIMARY KEY,
                trip_id VARCHAR,
                payment_date DATE,
                payment_method VARCHAR,
                amount_usd NUMERIC(10,2),
                tip_usd NUMERIC(8,2),
                status VARCHAR,
                auth_code VARCHAR,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        logger.info("✓ Bronze tables created (no FKs, no uniques)")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating bronze schema: {e}")
        return False


# ---------------- CREATE SILVER & GOLD ----------------
def create_silver_gold_views():
    """Create Silver (cleaned) and Gold (aggregated) views."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("CREATE SCHEMA IF NOT EXISTS silver")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS gold")
        logger.info("✓ Silver and Gold schemas created/verified")

        # Drop old views
        for v in ["driver_earnings", "rider_spending", "city_performance"]:
            cursor.execute(f"DROP VIEW IF EXISTS gold.{v} CASCADE")
        for e in ["drivers", "riders", "trips", "payments", "vehicles"]:
            cursor.execute(f"DROP VIEW IF EXISTS silver.{e}_clean CASCADE")

        # Silver views
        cursor.execute("""
            CREATE OR REPLACE VIEW silver.drivers_clean AS
            SELECT driver_id, INITCAP(TRIM(driver_name)) AS driver_name,
                   LOWER(TRIM(email)) AS email, dob, signup_date, driver_rating,
                   INITCAP(TRIM(city)) AS city, license_number,
                   COALESCE(is_active, TRUE) AS is_active,
                   created_at, updated_at
            FROM bronze.drivers
            WHERE driver_name IS NOT NULL AND email IS NOT NULL
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW silver.vehicles_clean AS
            SELECT vehicle_id, driver_id,
                   INITCAP(TRIM(make)) AS make,
                   INITCAP(TRIM(model)) AS model,
                   year, plate, capacity,
                   INITCAP(TRIM(color)) AS color,
                   registration_date,
                   COALESCE(is_active, TRUE) AS is_active,
                   created_at, updated_at
            FROM bronze.vehicles
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW silver.riders_clean AS
            SELECT rider_id, INITCAP(TRIM(rider_name)) AS rider_name,
                   LOWER(TRIM(email)) AS email,
                   signup_date, INITCAP(TRIM(home_city)) AS home_city,
                   rider_rating, default_payment_method,
                   COALESCE(is_verified, FALSE) AS is_verified,
                   created_at, updated_at
            FROM bronze.riders
            WHERE rider_name IS NOT NULL AND email IS NOT NULL
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW silver.trips_clean AS
            SELECT trip_id, rider_id, driver_id, vehicle_id,
                   request_ts, pickup_ts, dropoff_ts,
                   TRIM(pickup_location) AS pickup_location,
                   TRIM(drop_location) AS drop_location,
                   distance_km, duration_min, wait_time_minutes,
                   surge_multiplier, base_fare_usd, tax_usd,
                   COALESCE(tip_usd, 0) AS tip_usd,
                   total_fare_usd, status,
                   created_at, updated_at
            FROM bronze.trips
            WHERE status IS NOT NULL
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW silver.payments_clean AS
            SELECT payment_id, trip_id, payment_date,
                   payment_method, amount_usd,
                   COALESCE(tip_usd, 0) AS tip_usd,
                   status, auth_code,
                   created_at, updated_at
            FROM bronze.payments
            WHERE amount_usd > 0
        """)

        # Gold views
        cursor.execute("""
            CREATE OR REPLACE VIEW gold.driver_earnings AS
            SELECT d.driver_id, d.driver_name,
                   COUNT(t.trip_id) AS total_trips,
                   SUM(t.total_fare_usd) AS total_earnings
            FROM silver.drivers_clean d
            LEFT JOIN silver.trips_clean t ON d.driver_id = t.driver_id
            GROUP BY d.driver_id, d.driver_name
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW gold.rider_spending AS
            SELECT r.rider_id, r.rider_name,
                   COUNT(t.trip_id) AS total_trips,
                   SUM(t.total_fare_usd) AS total_spent,
                   AVG(t.total_fare_usd) AS avg_trip_cost
            FROM silver.riders_clean r
            LEFT JOIN silver.trips_clean t ON r.rider_id = t.rider_id
            GROUP BY r.rider_id, r.rider_name
        """)

        cursor.execute("""
            CREATE OR REPLACE VIEW gold.city_performance AS
            SELECT d.city,
                   COUNT(t.trip_id) AS total_trips,
                   SUM(t.total_fare_usd) AS total_revenue,
                   AVG(t.distance_km) AS avg_distance,
                   AVG(t.duration_min) AS avg_duration
            FROM silver.trips_clean t
            JOIN silver.drivers_clean d ON t.driver_id = d.driver_id
            GROUP BY d.city
        """)

        logger.info("✓ Silver and Gold layer views created")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating Silver/Gold views: {e}")
        return False


# ---------------- TEST ----------------
def test_connection():
    """Test connection and row counts for bronze tables."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        logger.info(f"✓ Connected to PostgreSQL: {version}")

        for table in ["drivers", "vehicles", "riders", "trips", "payments"]:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM bronze.{table}")
                count = cursor.fetchone()[0]
                logger.info(f"  ✓ bronze.{table}: {count:,} records")
            except psycopg2.Error as e:
                logger.error(f"  ❌ Error accessing bronze.{table}: {e}")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


# ---------------- MAIN ----------------
def main():
    logger.info("🚀 Setting up Medallion Database (Bronze → Silver → Gold)")
    logger.info("=" * 60)

    if not create_database():
        sys.exit(1)

    if not create_bronze_schema():
        sys.exit(1)

    if not create_silver_gold_views():
        logger.warning("⚠ Failed to create Silver/Gold views")

    if not test_connection():
        sys.exit(1)

    logger.info("\n🎉 Database setup completed successfully!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

    #