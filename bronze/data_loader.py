import os
import logging
import httplib2
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pathlib import Path
import sys
from datetime import datetime

# Add parent directory to path for config import
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES, LOG_CONFIG

# ---------------- Logging Setup ----------------
log_dir = Path(__file__).parent.parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_sheets_service():
    """Create and return Google Sheets service."""
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['credentials_path'],
            scopes=GOOGLE_SHEETS_CONFIG['scopes']
        )
        unverified_http = httplib2.Http(disable_ssl_certificate_validation=True)
        authorized_http = AuthorizedHttp(creds, http=unverified_http)
        service = build("sheets", "v4", http=authorized_http)
        logger.info("Google Sheets service created successfully")
        return service
    except Exception as e:
        logger.error(f"Error creating Google Sheets service: {e}")
        return None


def fetch_data(range_name):
    """Fetch rows from a Google Sheet range (skip headers)."""
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=range_name
        ).execute()
        values = result.get('values', [])[1:]  # skip header row
        logger.info(f"✓ Loaded {len(values)} rows from {range_name}")
        return values
    except Exception as e:
        logger.error(f"❌ Error fetching data from {range_name}: {e}")
        return []


def parse_date(value):
    """Convert sheet date to Python date (YYYY-MM-DD)."""
    if not value:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except Exception:
            continue
    return None


def parse_timestamp(value):
    """Convert sheet datetime to Python datetime."""
    if not value:
        return None
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue
    return None


def load_data(table, rows, conn):
    """Insert rows into the given Bronze table with conflict handling."""
    cursor = conn.cursor()

    if table == "drivers":
        query = """
            INSERT INTO bronze.drivers (
                driver_id, driver_name, email, dob, signup_date,
                driver_rating, city, license_number, is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (driver_id) DO NOTHING
        """
        data = [
            (
                r[0],  # driver_id
                r[1],  # driver_name
                r[2],  # email
                parse_date(r[3]),
                parse_date(r[4]),
                float(r[5]) if r[5] else None,
                r[6],
                r[7],
                r[8].lower() in ("true", "1", "yes") if len(r) > 8 and r[8] else None
            )
            for r in rows
        ]

    elif table == "vehicles":
        query = """
            INSERT INTO bronze.vehicles (
                vehicle_id, driver_id, make, model, year,
                plate, capacity, color, registration_date, is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (vehicle_id) DO NOTHING
        """
        data = [
            (
                r[0],  # vehicle_id
                r[1],  # driver_id
                r[2],  # make
                r[3],  # model
                int(r[4]) if r[4] else None,
                r[5],  # plate
                int(r[6]) if r[6] else None,
                r[7],  # color
                parse_date(r[8]) if len(r) > 8 else None,
                r[9].lower() in ("true", "1", "yes") if len(r) > 9 and r[9] else None
            )
            for r in rows
        ]

    elif table == "riders":
        query = """
            INSERT INTO bronze.riders (
                rider_id, rider_name, email, signup_date,
                home_city, rider_rating, default_payment_method, is_verified
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (rider_id) DO NOTHING
        """
        data = [
            (
                r[0],  # rider_id
                r[1],  # rider_name
                r[2],  # email
                parse_date(r[3]),
                r[4],  # home_city
                float(r[5]) if r[5] else None,
                r[6] if len(r) > 6 else None,
                r[7].lower() in ("true", "1", "yes") if len(r) > 7 and r[7] else None
            )
            for r in rows
        ]

    elif table == "trips":
        query = """
            INSERT INTO bronze.trips (
                trip_id, rider_id, driver_id, vehicle_id,
                request_ts, pickup_ts, dropoff_ts,
                pickup_location, drop_location,
                distance_km, duration_min, wait_time_minutes,
                surge_multiplier, base_fare_usd, tax_usd, tip_usd,
                total_fare_usd, status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (trip_id) DO NOTHING
        """
        data = [
            (
                r[0],  # trip_id
                r[1],  # rider_id
                r[2],  # driver_id
                r[3],  # vehicle_id
                parse_timestamp(r[4]),
                parse_timestamp(r[5]),
                parse_timestamp(r[6]),
                r[7],
                r[8],
                float(r[9]) if r[9] else None,
                float(r[10]) if r[10] else None,
                float(r[11]) if r[11] else None,
                float(r[12]) if r[12] else None,
                float(r[13]) if r[13] else None,
                float(r[14]) if r[14] else None,
                float(r[15]) if r[15] else None,
                float(r[16]) if r[16] else None,
                r[17] if len(r) > 17 else None
            )
            for r in rows
        ]

    elif table == "payments":
        query = """
            INSERT INTO bronze.payments (
                payment_id, trip_id, payment_date,
                payment_method, amount_usd, tip_usd,
                status, auth_code
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (payment_id) DO NOTHING
        """
        data = [
            (
                r[0],  # payment_id
                r[1],  # trip_id
                parse_date(r[2]),
                r[3],  # payment_method
                float(r[4]) if r[4] else None,
                float(r[5]) if r[5] else None,
                r[6],
                r[7] if len(r) > 7 else None
            )
            for r in rows
        ]

    else:
        logger.warning(f"⚠️ Unknown table: {table}")
        return

    try:
        cursor.executemany(query, data)
        conn.commit()
        logger.info(f"✓ Inserted {len(data)} rows into bronze.{table}")
    except Exception as e:
        logger.error(f"❌ Error inserting into {table}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def load_all_data_to_bronze():
    """
    Wrapper for orchestration layer.
    Loads all Google Sheets data into Bronze tables.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info("🥉 MEDALLION BRONZE LAYER - DATA LOADER")
        logger.info("🚀 Starting Bronze Data Pipeline")

        conn = psycopg2.connect(**DB_CONFIG)

        for table, sheet_range in SHEET_RANGES.items():
            rows = fetch_data(sheet_range)
            if rows:
                load_data(table, rows, conn)
            else:
                logger.warning(f"⚠️ No {table} data loaded")

        conn.close()
        logger.info("🎉 Bronze load completed")
        return True

    except Exception as e:
        logger.error(f"❌ Error in Bronze load: {e}")
        return False


def main():
    logger.info("🥉 MEDALLION BRONZE LAYER - DATA LOADER")
    logger.info("🚀 Starting Bronze Data Pipeline")

    try:
        conn = psycopg2.connect(**DB_CONFIG)

        for table, sheet_range in SHEET_RANGES.items():
            rows = fetch_data(sheet_range)
            if rows:
                load_data(table, rows, conn)
            else:
                logger.warning(f"⚠️ No {table} data loaded")

        conn.close()
        logger.info("🎉 Bronze load completed")

    except Exception as e:
        logger.error(f"❌ Database connection error: {e}")


if __name__ == "__main__":
    main()
#