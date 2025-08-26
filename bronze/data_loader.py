#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🥉 Bronze Layer Loader
Fetches data from Google Sheets and loads into Postgres bronze tables.
- Always refreshed: TRUNCATE + INSERT on every run
- Uses your config.py (DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES, LOG_CONFIG)
- Creates bronze schema & tables if missing
"""

import os
import logging
import httplib2
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from google_auth_httplib2 import AuthorizedHttp
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pathlib import Path
import sys
from datetime import datetime
import csv

# ------------------------------------------------------------
# Add parent directory to path for config import
# ------------------------------------------------------------
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, GOOGLE_SHEETS_CONFIG, SHEET_RANGES, LOG_CONFIG  # noqa: E402

# ------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------
log_dir = Path(__file__).parent.parent / LOG_CONFIG['log_dir']
log_dir.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(log_dir / 'data_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def safe_float(val):
    try:
        return float(val) if val not in (None, "", "NA") else None
    except Exception:
        return None


def safe_int(val):
    try:
        return int(val) if val not in (None, "", "NA") else None
    except Exception:
        return None


def safe_bool(val):
    if val in (None, "", "NA"):
        return None
    return str(val).strip().lower() in ("true", "1", "yes")


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


# ------------------------------------------------------------
# Google Sheets
# ------------------------------------------------------------
def get_sheets_service():
    """Create and return Google Sheets service (AuthorizedHttp, like your version)."""
    try:
        creds = Credentials.from_service_account_file(
            GOOGLE_SHEETS_CONFIG['credentials_path'],
            scopes=GOOGLE_SHEETS_CONFIG['scopes']
        )
        unverified_http = httplib2.Http(disable_ssl_certificate_validation=True)
        authorized_http = AuthorizedHttp(creds, http=unverified_http)
        service = build("sheets", "v4", http=authorized_http)
        logger.info("✅ Google Sheets service created successfully")
        return service
    except Exception as e:
        logger.error(f"❌ Error creating Google Sheets service: {e}")
        return None


def fetch_data(range_name):
    """Fetch rows from a Google Sheet range (skip headers)."""
    try:
        service = get_sheets_service()
        if service is None:
            return []
        result = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_CONFIG['spreadsheet_id'],
            range=range_name
        ).execute()
        values = result.get('values', [])
        if not values:
            logger.warning(f"⚠️ No data returned for {range_name}")
            return []
        rows = values[1:]  # skip header row
        logger.info(f"✓ Loaded {len(rows)} rows from {range_name}")
        return rows
    except Exception as e:
        logger.error(f"❌ Error fetching data from {range_name}: {e}")
        return []


# ------------------------------------------------------------
# DB Bootstrap
# ------------------------------------------------------------
DDL_TABLES = {
    "drivers": """
        CREATE TABLE IF NOT EXISTS bronze.drivers (
            driver_id TEXT PRIMARY KEY,
            driver_name TEXT,
            email TEXT,
            dob DATE,
            signup_date DATE,
            driver_rating NUMERIC,
            city TEXT,
            license_number TEXT,
            is_active BOOLEAN
        );
    """,
    "vehicles": """
        CREATE TABLE IF NOT EXISTS bronze.vehicles (
            vehicle_id TEXT PRIMARY KEY,
            driver_id TEXT,
            make TEXT,
            model TEXT,
            year INT,
            plate TEXT,
            capacity INT,
            color TEXT,
            registration_date DATE,
            is_active BOOLEAN
        );
    """,
    "riders": """
        CREATE TABLE IF NOT EXISTS bronze.riders (
            rider_id TEXT PRIMARY KEY,
            rider_name TEXT,
            email TEXT,
            signup_date DATE,
            home_city TEXT,
            rider_rating NUMERIC,
            default_payment_method TEXT,
            is_verified BOOLEAN
        );
    """,
    "trips": """
        CREATE TABLE IF NOT EXISTS bronze.trips (
            trip_id TEXT PRIMARY KEY,
            rider_id TEXT,
            driver_id TEXT,
            vehicle_id TEXT,
            request_ts TIMESTAMP,
            pickup_ts TIMESTAMP,
            dropoff_ts TIMESTAMP,
            pickup_location TEXT,
            drop_location TEXT,
            distance_km NUMERIC,
            duration_min NUMERIC,
            wait_time_minutes NUMERIC,
            surge_multiplier NUMERIC,
            base_fare_usd NUMERIC,
            tax_usd NUMERIC,
            tip_usd NUMERIC,
            total_fare_usd NUMERIC,
            status TEXT
        );
    """,
    "payments": """
        CREATE TABLE IF NOT EXISTS bronze.payments (
            payment_id TEXT PRIMARY KEY,
            trip_id TEXT,
            payment_date DATE,
            payment_method TEXT,
            amount_usd NUMERIC,
            tip_usd NUMERIC,
            status TEXT,
            auth_code TEXT
        );
    """
}


def ensure_bronze_schema_and_tables(conn):
    """Create schema and bronze tables if missing."""
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        for tname, ddl in DDL_TABLES.items():
            cur.execute(ddl)
    conn.commit()
    logger.info("✅ bronze schema and tables are ensured")


# ------------------------------------------------------------
# DB Loader
# ------------------------------------------------------------
def load_data(table, rows, conn):
    """TRUNCATE + INSERT rows into the given Bronze table."""
    cursor = conn.cursor()

    # Always refresh
    # CASCADE is safe if any downstream objects reference bronze tables (rare in bronze).
    cursor.execute(f"TRUNCATE TABLE bronze.{table} RESTART IDENTITY CASCADE;")

    if table == "drivers":
        query = """
            INSERT INTO bronze.drivers (
                driver_id, driver_name, email, dob, signup_date,
                driver_rating, city, license_number, is_active
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (driver_id) DO UPDATE SET
                driver_name = EXCLUDED.driver_name,
                email = EXCLUDED.email,
                dob = EXCLUDED.dob,
                signup_date = EXCLUDED.signup_date,
                driver_rating = EXCLUDED.driver_rating,
                city = EXCLUDED.city,
                license_number = EXCLUDED.license_number,
                is_active = EXCLUDED.is_active
        """
        data = [
            (
                r[0] if len(r) > 0 else None,
                r[1] if len(r) > 1 else None,
                r[2] if len(r) > 2 else None,
                parse_date(r[3]) if len(r) > 3 else None,
                parse_date(r[4]) if len(r) > 4 else None,
                safe_float(r[5]) if len(r) > 5 else None,
                r[6] if len(r) > 6 else None,
                r[7] if len(r) > 7 else None,
                safe_bool(r[8]) if len(r) > 8 else None
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
            ON CONFLICT (vehicle_id) DO UPDATE SET
                driver_id = EXCLUDED.driver_id,
                make = EXCLUDED.make,
                model = EXCLUDED.model,
                year = EXCLUDED.year,
                plate = EXCLUDED.plate,
                capacity = EXCLUDED.capacity,
                color = EXCLUDED.color,
                registration_date = EXCLUDED.registration_date,
                is_active = EXCLUDED.is_active
        """
        data = [
            (
                r[0] if len(r) > 0 else None,
                r[1] if len(r) > 1 else None,
                r[2] if len(r) > 2 else None,
                r[3] if len(r) > 3 else None,
                safe_int(r[4]) if len(r) > 4 else None,
                r[5] if len(r) > 5 else None,
                safe_int(r[6]) if len(r) > 6 else None,
                r[7] if len(r) > 7 else None,
                parse_date(r[8]) if len(r) > 8 else None,
                safe_bool(r[9]) if len(r) > 9 else None
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
            ON CONFLICT (rider_id) DO UPDATE SET
                rider_name = EXCLUDED.rider_name,
                email = EXCLUDED.email,
                signup_date = EXCLUDED.signup_date,
                home_city = EXCLUDED.home_city,
                rider_rating = EXCLUDED.rider_rating,
                default_payment_method = EXCLUDED.default_payment_method,
                is_verified = EXCLUDED.is_verified
        """
        data = [
            (
                r[0] if len(r) > 0 else None,
                r[1] if len(r) > 1 else None,
                r[2] if len(r) > 2 else None,
                parse_date(r[3]) if len(r) > 3 else None,
                r[4] if len(r) > 4 else None,
                safe_float(r[5]) if len(r) > 5 else None,
                r[6] if len(r) > 6 else None,
                safe_bool(r[7]) if len(r) > 7 else None
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
            ON CONFLICT (trip_id) DO UPDATE SET
                rider_id = EXCLUDED.rider_id,
                driver_id = EXCLUDED.driver_id,
                vehicle_id = EXCLUDED.vehicle_id,
                request_ts = EXCLUDED.request_ts,
                pickup_ts = EXCLUDED.pickup_ts,
                dropoff_ts = EXCLUDED.dropoff_ts,
                pickup_location = EXCLUDED.pickup_location,
                drop_location = EXCLUDED.drop_location,
                distance_km = EXCLUDED.distance_km,
                duration_min = EXCLUDED.duration_min,
                wait_time_minutes = EXCLUDED.wait_time_minutes,
                surge_multiplier = EXCLUDED.surge_multiplier,
                base_fare_usd = EXCLUDED.base_fare_usd,
                tax_usd = EXCLUDED.tax_usd,
                tip_usd = EXCLUDED.tip_usd,
                total_fare_usd = EXCLUDED.total_fare_usd,
                status = EXCLUDED.status
        """
        data = [
            (
                r[0] if len(r) > 0 else None,
                r[1] if len(r) > 1 else None,
                r[2] if len(r) > 2 else None,
                r[3] if len(r) > 3 else None,
                parse_timestamp(r[4]) if len(r) > 4 else None,
                parse_timestamp(r[5]) if len(r) > 5 else None,
                parse_timestamp(r[6]) if len(r) > 6 else None,
                r[7] if len(r) > 7 else None,
                r[8] if len(r) > 8 else None,
                safe_float(r[9]) if len(r) > 9 else None,
                safe_float(r[10]) if len(r) > 10 else None,
                safe_float(r[11]) if len(r) > 11 else None,
                safe_float(r[12]) if len(r) > 12 else None,
                safe_float(r[13]) if len(r) > 13 else None,
                safe_float(r[14]) if len(r) > 14 else None,
                safe_float(r[15]) if len(r) > 15 else None,
                safe_float(r[16]) if len(r) > 16 else None,
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
            ON CONFLICT (payment_id) DO UPDATE SET
                trip_id = EXCLUDED.trip_id,
                payment_date = EXCLUDED.payment_date,
                payment_method = EXCLUDED.payment_method,
                amount_usd = EXCLUDED.amount_usd,
                tip_usd = EXCLUDED.tip_usd,
                status = EXCLUDED.status,
                auth_code = EXCLUDED.auth_code
        """
        data = [
            (
                r[0] if len(r) > 0 else None,
                r[1] if len(r) > 1 else None,
                parse_date(r[2]) if len(r) > 2 else None,
                r[3] if len(r) > 3 else None,
                safe_float(r[4]) if len(r) > 4 else None,
                safe_float(r[5]) if len(r) > 5 else None,
                r[6] if len(r) > 6 else None,
                r[7] if len(r) > 7 else None
            )
            for r in rows
        ]

    else:
        logger.warning(f"⚠️ Unknown table: {table}")
        cursor.close()
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


# ------------------------------------------------------------
# CSV Saver (optional audit)
# ------------------------------------------------------------
def save_to_csv(table, rows, output_dir="bronze"):
    """Save fetched rows into a CSV file in the bronze folder (no header)."""
    try:
        output_path = Path(__file__).parent.parent / output_dir
        output_path.mkdir(exist_ok=True, parents=True)

        file_path = output_path / f"{table}.csv"

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        logger.info(f"📂 Saved {len(rows)} rows into {file_path}")
    except Exception as e:
        logger.error(f"❌ Error saving {table} CSV: {e}")


# ------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------
def load_all_data_to_bronze():
    """End-to-end run: bootstrap schema/tables, fetch each sheet, truncate+insert."""
    try:
        logger.info("🥉 MEDALLION BRONZE LAYER - DATA LOADER")
        logger.info("🚀 Starting Bronze Data Pipeline")

        conn = psycopg2.connect(**DB_CONFIG)
        ensure_bronze_schema_and_tables(conn)

        for table, sheet_range in SHEET_RANGES.items():
            rows = fetch_data(sheet_range)
            if rows:
                save_to_csv(table, rows)        # optional: keep for audit/debug
                load_data(table, rows, conn)    # TRUNCATE + INSERT fresh
            else:
                # Still hard refresh (empty state) so downstream is consistent
                logger.warning(f"⚠️ No {table} data loaded; clearing table to reflect sheet state")
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE bronze.{table} RESTART IDENTITY CASCADE;")
                    conn.commit()

        conn.close()
        logger.info("🎉 Bronze load completed (tables refreshed).")
        return True

    except Exception as e:
        logger.error(f"❌ Error in Bronze load: {e}", exc_info=True)
        return False


def main():
    load_all_data_to_bronze()


if __name__ == "__main__":
    main()
