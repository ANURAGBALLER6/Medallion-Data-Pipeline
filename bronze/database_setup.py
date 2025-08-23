import psycopg2
import sys
import logging
from pathlib import Path

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, LOG_CONFIG

# Set up logging
log_dir = Path(__file__).parent.parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'database_setup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def create_database():
    """Create the medallion_architecture database if it doesn't exist."""
    default_config = DB_CONFIG.copy()
    default_config['database'] = 'postgres'

    try:
        conn = psycopg2.connect(**default_config)
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'medallion_architecture'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE medallion_architecture")
            logger.info("✓ Database 'medallion_architecture' created successfully")
        else:
            logger.info("✓ Database 'medallion_architecture' already exists")

        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating database: {e}")
        return False


def create_bronze_schema():
    """Create bronze schema and tables for drivers, vehicles, riders, trips, payments."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create bronze schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        logger.info("✓ Bronze schema created/verified")

        # Create drivers table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.drivers (
                driver_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT,
                email TEXT,
                license_number TEXT,
                rating NUMERIC(3,2),
                city TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✓ Table 'bronze.drivers' created/verified")

        # Create vehicles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.vehicles (
                vehicle_id TEXT PRIMARY KEY,
                driver_id TEXT,
                make TEXT,
                model TEXT,
                year INT,
                plate_number TEXT,
                color TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✓ Table 'bronze.vehicles' created/verified")

        # Create riders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.riders (
                rider_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT,
                email TEXT,
                city TEXT,
                rating NUMERIC(3,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✓ Table 'bronze.riders' created/verified")

        # Create trips table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.trips (
                trip_id TEXT PRIMARY KEY,
                driver_id TEXT,
                rider_id TEXT,
                vehicle_id TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                start_location TEXT,
                end_location TEXT,
                distance_km NUMERIC(10,2),
                duration_min NUMERIC(10,2),
                fare_amount NUMERIC(10,2),
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✓ Table 'bronze.trips' created/verified")

        # Create payments table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bronze.payments (
                payment_id TEXT PRIMARY KEY,
                trip_id TEXT,
                rider_id TEXT,
                driver_id TEXT,
                amount NUMERIC(10,2),
                payment_method TEXT,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✓ Table 'bronze.payments' created/verified")

        conn.commit()
        cursor.close()
        conn.close()
        return True

    except psycopg2.Error as e:
        logger.error(f"❌ Error creating bronze schema/tables: {e}")
        return False


def main():
    logger.info("🚀 Setting up Medallion Database - Bronze Layer")
    logger.info("=" * 60)

    logger.info("1. Creating database...")
    if not create_database():
        sys.exit(1)

    logger.info("2. Creating bronze schema and tables...")
    if not create_bronze_schema():
        sys.exit(1)

    logger.info("\n🎉 Bronze Layer Database Setup Completed Successfully!")
    logger.info("=" * 60)
    logger.info("Next step: Run bronze/data_loader.py to load data")
    logger.info(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")


if __name__ == "__main__":
    main()
