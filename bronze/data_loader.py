import logging
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from psycopg2 import connect
from psycopg2.extras import execute_batch
from config import DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES, LOG_CONFIG
import os

# Setup logging
os.makedirs(LOG_CONFIG['log_dir'], exist_ok=True)
logging.basicConfig(
    level=LOG_CONFIG['level'],
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(os.path.join(LOG_CONFIG['log_dir'], "bronze_loader.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ---------- Helpers ----------
def get_db_connection():
    try:
        conn = connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None


def get_gsheet_data(sheet_range):
    try:
        creds = service_account.Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['credentials_path'],
            scopes=GOOGLE_SHEETS_CONFIG['scopes']
        )
        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=sheet_range
        ).execute()
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()

        df = pd.DataFrame(values[1:], columns=values[0])

        # normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        return df
    except Exception as e:
        logger.error(f"Error fetching data from Google Sheets ({sheet_range}): {e}")
        return pd.DataFrame()


def parse_id(val, prefix):
    """Convert IDs like D0001 → 1, T0070812 → 70812"""
    if pd.isna(val):
        return None
    s = str(val).strip()
    if not s:
        return None
    return int(s.replace(prefix, "").lstrip("0") or 0)


# ---------- Loaders ----------
def load_drivers_to_bronze(df):
    if df.empty:
        logger.warning("No drivers data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.drivers (driver_name, email, phone, license_number, city, status, rating)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (email) DO UPDATE SET
            driver_name = EXCLUDED.driver_name,
            phone = EXCLUDED.phone,
            license_number = EXCLUDED.license_number,
            city = EXCLUDED.city,
            status = EXCLUDED.status,
            rating = EXCLUDED.rating,
            created_at = CURRENT_TIMESTAMP;
        """
        data = [
            (
                row.get('driver_name'),
                row.get('email'),
                row.get('phone') or row.get('phone_number'),
                row.get('license_number'),
                row.get('city'),
                row.get('status'),
                float(row.get('rating')) if row.get('rating') else None
            )
            for _, row in df.iterrows()
        ]
        execute_batch(cursor, insert_query, data)
        conn.commit()
        logger.info(f"✅ Loaded {len(df)} drivers")
        return True
    except Exception as e:
        logger.error(f"Error loading drivers: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_vehicles_to_bronze(df):
    if df.empty:
        logger.warning("No vehicles data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.vehicles (driver_id, make, model, year, plate_number, color, status, capacity)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (plate_number) DO UPDATE SET
            driver_id = EXCLUDED.driver_id,
            make = EXCLUDED.make,
            model = EXCLUDED.model,
            year = EXCLUDED.year,
            color = EXCLUDED.color,
            status = EXCLUDED.status,
            capacity = EXCLUDED.capacity,
            created_at = CURRENT_TIMESTAMP;
        """
        data = [
            (
                parse_id(row.get('driver_id'), "D"),
                row.get('make'),
                row.get('model'),
                int(row.get('year')) if row.get('year') else None,
                row.get('plate_number'),
                row.get('color'),
                row.get('status'),
                int(row.get('capacity')) if row.get('capacity') else None
            )
            for _, row in df.iterrows()
        ]
        execute_batch(cursor, insert_query, data)
        conn.commit()
        logger.info(f"✅ Loaded {len(df)} vehicles")
        return True
    except Exception as e:
        logger.error(f"Error loading vehicles: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_riders_to_bronze(df):
    if df.empty:
        logger.warning("No riders data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.riders (rider_name, email, phone, city, status)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (email) DO UPDATE SET
            rider_name = EXCLUDED.rider_name,
            phone = EXCLUDED.phone,
            city = EXCLUDED.city,
            status = EXCLUDED.status,
            created_at = CURRENT_TIMESTAMP;
        """
        data = [
            (
                row.get('rider_name'),
                row.get('email'),
                row.get('phone') or row.get('phone_number'),
                row.get('city'),
                row.get('status')
            )
            for _, row in df.iterrows()
        ]
        execute_batch(cursor, insert_query, data)
        conn.commit()
        logger.info(f"✅ Loaded {len(df)} riders")
        return True
    except Exception as e:
        logger.error(f"Error loading riders: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_trips_to_bronze(df):
    if df.empty:
        logger.warning("No trips data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.trips (driver_id, rider_id, vehicle_id, start_location, end_location,
                                  start_time, end_time, distance_km, fare, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        data = [
            (
                parse_id(row.get('driver_id'), "D"),
                parse_id(row.get('rider_id'), "R"),
                parse_id(row.get('vehicle_id'), "V"),
                row.get('start_location'),
                row.get('end_location'),
                row.get('start_time'),
                row.get('end_time'),
                float(row.get('distance_km')) if row.get('distance_km') else None,
                float(row.get('fare')) if row.get('fare') else None,
                row.get('status')
            )
            for _, row in df.iterrows()
        ]
        execute_batch(cursor, insert_query, data)
        conn.commit()
        logger.info(f"✅ Loaded {len(df)} trips")
        return True
    except Exception as e:
        logger.error(f"Error loading trips: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def load_payments_to_bronze(df):
    if df.empty:
        logger.warning("No payments data to load")
        return False

    conn = get_db_connection()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO bronze.payments (trip_id, rider_id, amount, method, status, paid_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        """
        data = [
            (
                parse_id(row.get('trip_id'), "T"),
                parse_id(row.get('rider_id'), "R"),
                float(row.get('amount')) if row.get('amount') else None,
                row.get('method'),
                row.get('status'),
                row.get('paid_at')
            )
            for _, row in df.iterrows()
        ]
        execute_batch(cursor, insert_query, data)
        conn.commit()
        logger.info(f"✅ Loaded {len(df)} payments")
        return True
    except Exception as e:
        logger.error(f"Error loading payments: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


# ---------- Main ----------
if __name__ == "__main__":
    logger.info("🥉 MEDALLION BRONZE LAYER - DATA LOADER")
    logger.info("🚀 Starting Bronze Data Pipeline")

    results = {}

    results['drivers'] = load_drivers_to_bronze(get_gsheet_data(SHEET_RANGES['drivers']))
    results['vehicles'] = load_vehicles_to_bronze(get_gsheet_data(SHEET_RANGES['vehicles']))
    results['riders'] = load_riders_to_bronze(get_gsheet_data(SHEET_RANGES['riders']))
    results['trips'] = load_trips_to_bronze(get_gsheet_data(SHEET_RANGES['trips']))
    results['payments'] = load_payments_to_bronze(get_gsheet_data(SHEET_RANGES['payments']))

    logger.info("\n📊 BRONZE LAYER SUMMARY")
    for k, v in results.items():
        logger.info(f"{k:<10}: {'✅' if v else '❌'}")
    logger.info("🎉 Bronze load completed")
