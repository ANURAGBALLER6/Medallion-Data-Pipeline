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

# Logging setup
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
    try:
        service = get_sheets_service()
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=range_name
        ).execute()
        values = result.get('values', [])[1:]  # skip header
        logger.info(f"✓ Loaded {len(values)} rows from {range_name}")
        return values
    except Exception as e:
        logger.error(f"❌ Error fetching data from {range_name}: {e}")
        return []


def parse_date(value):
    """Convert Google Sheet date string M/D/YYYY to ISO format YYYY-MM-DD."""
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except Exception:
        return None


def load_data(table, rows, conn):
    cursor = conn.cursor()
    if table == "drivers":
        query = """
            INSERT INTO bronze.drivers (driver_id, name, email, license_number, rating, city)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (driver_id) DO NOTHING
        """
        data = [(r[0], r[1], r[2], r[7], r[5], r[6]) for r in rows]

    elif table == "vehicles":
        query = """
            INSERT INTO bronze.vehicles (vehicle_id, driver_id, make, model, year, plate_number, color)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (vehicle_id) DO NOTHING
        """
        data = [(r[0], r[1], r[2], r[3], r[4], r[5], r[7]) for r in rows]

    elif table == "riders":
        query = """
            INSERT INTO bronze.riders (rider_id, name, email, city, rating)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (rider_id) DO NOTHING
        """
        data = [(r[0], r[1], r[2], r[4], r[5]) for r in rows]

    elif table == "trips":
        query = """
            INSERT INTO bronze.trips (trip_id, driver_id, rider_id, vehicle_id,
                                      start_time, end_time, start_location, end_location,
                                      distance_km, duration_min, fare_amount, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
        """
        data = [
            (
                r[0],
                r[2],
                r[1],
                r[3],
                parse_date(r[5]),
                parse_date(r[6]),
                r[7],
                r[8],
                float(r[9]) if r[9] else None,
                float(r[10]) if r[10] else None,
                float(r[15]) if r[15] else None,
                r[16],
            )
            for r in rows
        ]

    elif table == "payments":
        query = """
            INSERT INTO bronze.payments (payment_id, trip_id, amount, payment_method, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (payment_id) DO NOTHING
        """
        data = [(r[0], r[1], r[4], r[3], r[6]) for r in rows]

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
