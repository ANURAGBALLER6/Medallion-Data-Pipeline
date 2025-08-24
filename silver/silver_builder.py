"""
Silver Layer Builder for Medallion Data Pipeline
Handles data cleaning, validation, and quality checks (ride-sharing domain)
"""

from __future__ import annotations

import logging
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path
from urllib.parse import quote_plus
from typing import Tuple, List, Dict

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.types import Text, Date, Integer, Numeric

# ---------------- Config import ----------------
# Expecting config.py at project root (one level up) that exposes DB_CONFIG, LOG_CONFIG
# DB_CONFIG = {"user":"...", "password":"...", "host":"...", "port":5432, "database":"..."}
# LOG_CONFIG = {"level":"INFO", "format":"...", "log_dir":"logs"}
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, LOG_CONFIG  # noqa: E402

# ---------------- Logging ----------------
log_dir = Path(__file__).parent.parent / LOG_CONFIG.get("log_dir", "logs")
log_dir.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG.get("level", "INFO")),
    format=LOG_CONFIG.get("format", "%(asctime)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.FileHandler(log_dir / "silver_builder.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ---------------- Engine (URL-safe for @ in password) ----------------
def make_engine() -> Engine:
    user = quote_plus(DB_CONFIG["user"])
    pwd = quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    db = DB_CONFIG["database"]
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # future=True ensures 2.0 style behavior
    return create_engine(url, future=True)


engine = make_engine()


def run_sql(sql: str, params: dict | None = None):
    """Execute arbitrary SQL in its own transaction."""
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


class SilverBuilder:
    """Handles Silver layer ETL operations."""

    def __init__(self):
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        # Write parents before children to avoid dependency surprises
        self.tables = ['drivers', 'vehicles', 'riders', 'trips', 'payments']
        self.stats: Dict[str, Dict[str, int]] = {}

    # ---------------- Step 1: Schemas + Audit ----------------
    def setup_schemas(self) -> bool:
        logger.info("Setting up Silver and Audit schemas...")
        try:
            run_sql("CREATE SCHEMA IF NOT EXISTS silver;")
            run_sql("CREATE SCHEMA IF NOT EXISTS audit;")

            run_sql("""
                CREATE TABLE IF NOT EXISTS audit.rejected_rows (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    record JSONB NOT NULL,
                    reason TEXT NOT NULL,
                    run_id VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            run_sql("""
                CREATE TABLE IF NOT EXISTS audit.dq_results (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(100) NOT NULL,
                    check_name VARCHAR(200) NOT NULL,
                    pass_fail BOOLEAN NOT NULL,
                    bad_row_count INTEGER DEFAULT 0,
                    run_id VARCHAR(50) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            run_sql("""
                CREATE TABLE IF NOT EXISTS audit.etl_log (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50) NOT NULL,
                    run_timestamp TIMESTAMP NOT NULL,
                    step_executed VARCHAR(100) NOT NULL,
                    table_name VARCHAR(100),
                    input_row_count INTEGER,
                    output_row_count INTEGER,
                    rejected_row_count INTEGER,
                    data_checksum VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            logger.info("✅ Schemas and audit tables ready")
            return True
        except Exception as e:
            logger.error(f"❌ Error setting up schemas: {e}")
            return False

    # ---------------- Step 2: Base tables (Bronze -> Silver _base) ----------------
    def create_silver_base_tables(self) -> bool:
        logger.info("Creating Silver base tables with light cleaning...")

        # All IDs are kept as TEXT to allow alphanumeric keys like "P0000001"
        sql_scripts: Dict[str, str] = {
            'drivers': """
                DROP TABLE IF EXISTS silver.drivers_base;
                CREATE TABLE silver.drivers_base AS
                WITH cleaned AS (
                    SELECT
                        TRIM(driver_id::TEXT) AS driver_id,   -- TEXT
                        TRIM(driver_name::TEXT) AS driver_name,
                        LOWER(TRIM(email::TEXT)) AS email,
                        NULLIF(TRIM(dob::TEXT), '')::DATE AS dob,
                        NULLIF(TRIM(signup_date::TEXT), '')::DATE AS signup_date,
                        CASE WHEN driver_rating IS NULL THEN NULL
                             WHEN driver_rating::NUMERIC BETWEEN 0 AND 5 THEN driver_rating::NUMERIC
                             ELSE NULL END AS driver_rating,
                        TRIM(city::TEXT) AS city,
                        TRIM(license_number::TEXT) AS license_number,
                        CASE WHEN LOWER(COALESCE(is_active::TEXT,'')) IN ('true','1','yes') THEN TRUE
                             WHEN LOWER(COALESCE(is_active::TEXT,'')) IN ('false','0','no') THEN FALSE
                             ELSE NULL END AS is_active,
                        ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY signup_date DESC NULLS LAST) AS rn
                    FROM bronze.drivers
                    WHERE driver_id IS NOT NULL
                )
                SELECT driver_id, driver_name, email, dob, signup_date, driver_rating,
                       city, license_number, is_active
                FROM cleaned WHERE rn = 1;
            """,

            'vehicles': """
                DROP TABLE IF EXISTS silver.vehicles_base;
                CREATE TABLE silver.vehicles_base AS
                WITH cleaned AS (
                    SELECT
                        TRIM(vehicle_id::TEXT) AS vehicle_id,  -- TEXT
                        TRIM(driver_id::TEXT) AS driver_id,    -- TEXT
                        INITCAP(TRIM(make::TEXT)) AS make,
                        INITCAP(TRIM(model::TEXT)) AS model,
                        NULLIF(year::TEXT,'')::INT AS year,
                        UPPER(TRIM(plate::TEXT)) AS plate,
                        NULLIF(capacity::TEXT,'')::INT AS capacity,
                        INITCAP(TRIM(color::TEXT)) AS color,
                        NULLIF(TRIM(registration_date::TEXT),'')::DATE AS registration_date,
                        CASE WHEN LOWER(COALESCE(is_active::TEXT,'')) IN ('true','1','yes') THEN TRUE
                             WHEN LOWER(COALESCE(is_active::TEXT,'')) IN ('false','0','no') THEN FALSE
                             ELSE NULL END AS is_active,
                        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY registration_date DESC NULLS LAST) AS rn
                    FROM bronze.vehicles
                    WHERE vehicle_id IS NOT NULL
                )
                SELECT vehicle_id, driver_id, make, model, year, plate, capacity, color, registration_date, is_active
                FROM cleaned WHERE rn = 1;
            """,

            'riders': """
                DROP TABLE IF EXISTS silver.riders_base;
                CREATE TABLE silver.riders_base AS
                WITH cleaned AS (
                    SELECT
                        TRIM(rider_id::TEXT) AS rider_id,  -- TEXT
                        TRIM(rider_name::TEXT) AS rider_name,
                        LOWER(TRIM(email::TEXT)) AS email,
                        NULLIF(TRIM(signup_date::TEXT),'')::DATE AS signup_date,
                        INITCAP(TRIM(home_city::TEXT)) AS home_city,
                        CASE WHEN rider_rating IS NULL THEN NULL
                             WHEN rider_rating::NUMERIC BETWEEN 0 AND 5 THEN rider_rating::NUMERIC
                             ELSE NULL END AS rider_rating,
                        TRIM(default_payment_method::TEXT) AS default_payment_method,
                        CASE WHEN LOWER(COALESCE(is_verified::TEXT,'')) IN ('true','1','yes') THEN TRUE
                             WHEN LOWER(COALESCE(is_verified::TEXT,'')) IN ('false','0','no') THEN FALSE
                             ELSE NULL END AS is_verified,
                        ROW_NUMBER() OVER (PARTITION BY rider_id ORDER BY signup_date DESC NULLS LAST) AS rn
                    FROM bronze.riders
                    WHERE rider_id IS NOT NULL
                )
                SELECT rider_id, rider_name, email, signup_date, home_city, rider_rating,
                       default_payment_method, is_verified
                FROM cleaned WHERE rn = 1;
            """,

            'trips': """
                DROP TABLE IF EXISTS silver.trips_base;
                CREATE TABLE silver.trips_base AS
                WITH cleaned AS (
                    SELECT
                        TRIM(trip_id::TEXT) AS trip_id,      -- TEXT
                        TRIM(rider_id::TEXT) AS rider_id,    -- TEXT
                        TRIM(driver_id::TEXT) AS driver_id,  -- TEXT
                        TRIM(vehicle_id::TEXT) AS vehicle_id, -- TEXT
                        NULLIF(TRIM(request_ts::TEXT),'')::TIMESTAMP AS request_ts,
                        NULLIF(TRIM(pickup_ts::TEXT),'')::TIMESTAMP AS pickup_ts,
                        NULLIF(TRIM(dropoff_ts::TEXT),'')::TIMESTAMP AS dropoff_ts,
                        TRIM(pickup_location::TEXT) AS pickup_location,
                        TRIM(drop_location::TEXT) AS drop_location,
                        NULLIF(distance_km::TEXT,'')::NUMERIC AS distance_km,
                        NULLIF(duration_min::TEXT,'')::NUMERIC AS duration_min,
                        NULLIF(wait_time_minutes::TEXT,'')::NUMERIC AS wait_time_minutes,
                        NULLIF(surge_multiplier::TEXT,'')::NUMERIC AS surge_multiplier,
                        NULLIF(base_fare_usd::TEXT,'')::NUMERIC AS base_fare_usd,
                        NULLIF(tax_usd::TEXT,'')::NUMERIC AS tax_usd,
                        NULLIF(tip_usd::TEXT,'')::NUMERIC AS tip_usd,
                        NULLIF(total_fare_usd::TEXT,'')::NUMERIC AS total_fare_usd,
                        INITCAP(TRIM(status::TEXT)) AS status,
                        ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY request_ts DESC NULLS LAST) AS rn
                    FROM bronze.trips
                    WHERE trip_id IS NOT NULL
                )
                SELECT trip_id, rider_id, driver_id, vehicle_id, request_ts, pickup_ts, dropoff_ts,
                       pickup_location, drop_location, distance_km, duration_min, wait_time_minutes,
                       surge_multiplier, base_fare_usd, tax_usd, tip_usd, total_fare_usd, status
                FROM cleaned WHERE rn = 1;
            """,

            'payments': """
                DROP TABLE IF EXISTS silver.payments_base;
                CREATE TABLE silver.payments_base AS
                WITH cleaned AS (
                    SELECT
                        TRIM(payment_id::TEXT) AS payment_id, -- TEXT
                        TRIM(trip_id::TEXT) AS trip_id,       -- TEXT
                        NULLIF(TRIM(payment_date::TEXT),'')::DATE AS payment_date,
                        TRIM(payment_method::TEXT) AS payment_method,
                        NULLIF(amount_usd::TEXT,'')::NUMERIC AS amount_usd,
                        NULLIF(tip_usd::TEXT,'')::NUMERIC AS tip_usd,
                        INITCAP(TRIM(status::TEXT)) AS status,
                        TRIM(auth_code::TEXT) AS auth_code,
                        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_date DESC NULLS LAST) AS rn
                    FROM bronze.payments
                    WHERE payment_id IS NOT NULL
                )
                SELECT payment_id, trip_id, payment_date, payment_method, amount_usd, tip_usd, status, auth_code
                FROM cleaned WHERE rn = 1;
            """
        }

        try:
            for t, sql in sql_scripts.items():
                logger.info(f"Creating silver.{t}_base ...")
                run_sql(sql)
                with engine.connect() as conn:
                    cnt = conn.execute(text(f"SELECT COUNT(*) FROM silver.{t}_base")).scalar_one()
                logger.info(f"✅ silver.{t}_base created with {cnt:,} rows")
                self.log_etl_step(f"create_base_{t}", t, None, cnt, 0)
            logger.info("✅ All Silver base tables created")
            return True
        except Exception as e:
            logger.error(f"❌ Error creating Silver base tables: {e}")
            return False

    # ---------------- Step 3: Deep validation (Pandas) ----------------
    def deep_validation(self) -> bool:
        logger.info("Performing deep validation...")
        ok = True
        for t in self.tables:
            logger.info(f"Validating {t}...")
            if not self._validate_table(t):
                ok = False
        if ok:
            logger.info("✅ All tables passed deep validation")
        else:
            logger.warning("⚠️  Some tables had validation issues")
        return ok

    def _validate_table(self, table_name: str) -> bool:
        try:
            df = pd.read_sql(f"SELECT * FROM silver.{table_name}_base", engine)
            logger.info(f"Loaded {len(df):,} rows for validation: silver.{table_name}_base")

            if df.empty:
                self.stats[table_name] = {'input_rows': 0, 'valid_rows': 0, 'invalid_rows': 0}
                logger.warning(f"No data found in silver.{table_name}_base")
                return True

            # Normalize columns for validations where helpful (non-destructive)
            if table_name == 'payments' and 'payment_method' in df.columns:
                df['payment_method_norm'] = self._normalize_payment_method_series(df['payment_method'])
            else:
                df['payment_method_norm'] = None  # placeholder when not used

            valid_df, invalid_df, reasons = self._apply_table_validations(table_name, df)

            # Persist valid data into final silver table
            if not valid_df.empty:
                # Dtype hints to keep text IDs as TEXT
                dtype_map = {}
                if table_name == 'drivers':
                    dtype_map = {
                        'driver_id': Text(), 'driver_name': Text(), 'email': Text(),
                        'city': Text(), 'license_number': Text()
                    }
                elif table_name == 'vehicles':
                    dtype_map = {
                        'vehicle_id': Text(), 'driver_id': Text(), 'make': Text(),
                        'model': Text(), 'plate': Text(), 'color': Text()
                    }
                elif table_name == 'riders':
                    dtype_map = {
                        'rider_id': Text(), 'rider_name': Text(), 'email': Text(),
                        'home_city': Text(), 'default_payment_method': Text()
                    }
                elif table_name == 'trips':
                    dtype_map = {
                        'trip_id': Text(), 'rider_id': Text(), 'driver_id': Text(),
                        'vehicle_id': Text(), 'pickup_location': Text(), 'drop_location': Text(),
                        'status': Text()
                    }
                elif table_name == 'payments':
                    dtype_map = {
                        'payment_id': Text(), 'trip_id': Text(), 'payment_method': Text(),
                        'status': Text(), 'auth_code': Text()
                    }

                # Drop helper column if present
                if 'payment_method_norm' in valid_df.columns:
                    valid_df = valid_df.drop(columns=['payment_method_norm'], errors='ignore')

                valid_df.to_sql(
                    table_name, engine, schema='silver',
                    if_exists='replace', index=False, dtype=dtype_map
                )
                logger.info(f"✅ {len(valid_df):,} valid rows saved to silver.{table_name}")
            else:
                logger.warning(f"⚠️ No valid rows to write for {table_name}")

            # Save invalid rows to audit
            if not invalid_df.empty:
                self._save_rejected_rows(table_name, invalid_df, reasons)
                logger.warning(f"⚠️  {len(invalid_df):,} invalid rows saved to audit.rejected_rows")

            # Update stats
            self.stats[table_name] = {
                'input_rows': len(df),
                'valid_rows': len(valid_df),
                'invalid_rows': len(invalid_df)
            }

            self.log_etl_step(
                f"deep_validation_{table_name}", table_name,
                len(df), len(valid_df), len(invalid_df)
            )
            return True
        except Exception as e:
            logger.error(f"Error validating {table_name}: {e}")
            return False

    def _normalize_payment_method_series(self, s: pd.Series) -> pd.Series:
        """
        Normalize payment method values to a canonical small set.
        Examples mapped to: Card, Cash, Wallet, UPI
        """
        if s is None:
            return pd.Series(dtype=object)

        mapping = {
            'credit card': 'Card',
            'debit card': 'Card',
            'card': 'Card',
            'cash': 'Cash',
            'wallet': 'Wallet',
            'upi': 'UPI',
            'gpay': 'UPI',
            'google pay': 'UPI',
            'phonepe': 'UPI',
            'paytm': 'UPI',
            'apple pay': 'Card',   # treat as card-rails for now
            'mastercard': 'Card',
            'visa': 'Card'
        }
        def norm(x):
            if x is None:
                return None
            t = str(x).strip().lower()
            return mapping.get(t, str(x).strip().title())  # default Title Case (e.g., "Upi" -> "Upi")
        return s.apply(norm)

    def _apply_table_validations(self, table_name: str, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        valid_mask = pd.Series(True, index=df.index)
        reasons = [''] * len(df)

        def add_reason(mask: pd.Series, msg: str):
            nonlocal valid_mask, reasons
            # for rows where mask is True (bad rows), append reason
            for i, bad in mask.items():
                if bad:
                    reasons[i] = (reasons[i] + '; ' if reasons[i] else '') + msg
            valid_mask &= ~mask

        if table_name == 'drivers':
            email_pat = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            add_reason(~df['email'].fillna('').str.match(email_pat), 'Invalid email')
            add_reason(~df['license_number'].notna(), 'Missing license number')
            add_reason(~df['driver_rating'].fillna(0).between(0, 5), 'Driver rating out of range (0-5)')

        elif table_name == 'vehicles':
            current_year = datetime.now().year
            add_reason(~df['driver_id'].notna(), 'Missing driver_id')
            add_reason(~df['year'].fillna(0).between(1980, current_year + 1), f'Invalid year (1980-{current_year+1})')
            add_reason(~df['capacity'].fillna(0).between(1, 8), 'Capacity out of range (1-8)')
            add_reason(~df['plate'].fillna('').str.match(r'^[A-Z0-9\-]{3,12}$'), 'Invalid plate')

        elif table_name == 'riders':
            email_pat = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
            add_reason(~df['email'].fillna('').str.match(email_pat), 'Invalid email')
            add_reason(~df['rider_rating'].fillna(0).between(0, 5), 'Rider rating out of range (0-5)')

        elif table_name == 'trips':
            add_reason(~df['rider_id'].notna(), 'Missing rider_id')
            add_reason(~df['driver_id'].notna(), 'Missing driver_id')
            add_reason(~df['vehicle_id'].notna(), 'Missing vehicle_id')
            # time logic
            add_reason((df['pickup_ts'].notna()) & (df['request_ts'].notna()) & (df['pickup_ts'] < df['request_ts']),
                       'pickup_ts before request_ts')
            add_reason((df['dropoff_ts'].notna()) & (df['pickup_ts'].notna()) & (df['dropoff_ts'] < df['pickup_ts']),
                       'dropoff_ts before pickup_ts')
            # non-negatives
            for col, label in [
                ('distance_km', 'distance_km'),
                ('duration_min', 'duration_min'),
                ('wait_time_minutes', 'wait_time_minutes'),
                ('base_fare_usd', 'base_fare_usd'),
                ('tax_usd', 'tax_usd'),
                ('tip_usd', 'tip_usd'),
                ('total_fare_usd', 'total_fare_usd')
            ]:
                add_reason(df[col].fillna(0) < 0, f'Negative {label}')
            # fare sanity (allow tiny float noise)
            if {'base_fare_usd', 'tax_usd', 'tip_usd', 'total_fare_usd'}.issubset(df.columns):
                add_reason(
                    (df[['base_fare_usd', 'tax_usd', 'tip_usd']].fillna(0).sum(axis=1) - df['total_fare_usd'].fillna(0)).abs() > 1e-6,
                    'total_fare_usd != base+tax+tip'
                )

        elif table_name == 'payments':
            # prefer normalized method if present
            pm_series = (df['payment_method_norm']
                         if 'payment_method_norm' in df.columns
                         else df['payment_method'])
            add_reason(~df['trip_id'].notna(), 'Missing trip_id')
            add_reason(df['amount_usd'].fillna(0) < 0, 'Negative amount_usd')
            add_reason(df['tip_usd'].fillna(0) < 0, 'Negative tip_usd')
            allowed = {'Card', 'Cash', 'Wallet', 'UPI'}
            add_reason(~pm_series.fillna('').isin(allowed), 'Unknown payment_method')

            # If normalized value is good but original is weird casing, fix it for valid rows
            if 'payment_method_norm' in df.columns:
                needs_fix = pm_series.notna() & pm_series.isin(allowed)
                df.loc[needs_fix, 'payment_method'] = df.loc[needs_fix, 'payment_method_norm']

        valid_df = df[valid_mask].copy()
        invalid_df = df[~valid_mask].copy()
        invalid_reasons = [reasons[i] for i in invalid_df.index]

        # Drop helper column from both before saving/inserting
        for d in (valid_df, invalid_df):
            if 'payment_method_norm' in d.columns:
                d.drop(columns=['payment_method_norm'], inplace=True, errors='ignore')

        return valid_df, invalid_df, invalid_reasons

    def _save_rejected_rows(self, table_name: str, invalid_df: pd.DataFrame, reasons: List[str]):
        """
        Persist rejected rows into audit.rejected_rows using uniform named params
        and server-side cast to JSONB to avoid psycopg param style pitfalls.
        """
        try:
            if invalid_df.empty:
                return
            rows = invalid_df.to_dict(orient='records')
            payload = [
                {
                    "t": table_name,
                    "r": pd.io.json.dumps(rec),  # JSON text
                    "reason": reasons[idx] if idx < len(reasons) else "Validation failed",
                    "run": self.run_id
                }
                for idx, rec in enumerate(rows)
            ]
            with engine.begin() as conn:
                for item in payload:
                    conn.execute(
                        text("""
                            INSERT INTO audit.rejected_rows (table_name, record, reason, run_id)
                            VALUES (:t, CAST(:r AS JSONB), :reason, :run)
                        """),
                        item
                    )
        except Exception as e:
            logger.error(f"Error saving rejected rows for {table_name}: {e}")

    # ---------------- Step 4: Data Quality checks ----------------
    def run_data_quality_checks(self) -> bool:
        logger.info("Running Data Quality checks...")

        checks: Dict[str, List[Tuple[str, str]]] = {
            'drivers': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT driver_id) FROM silver.drivers"),
                ('email_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT email) FROM silver.drivers WHERE email IS NOT NULL")
            ],
            'vehicles': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT vehicle_id) FROM silver.vehicles"),
                ('fk_driver', """SELECT COUNT(*) FROM silver.vehicles v
                                 LEFT JOIN silver.drivers d ON v.driver_id = d.driver_id
                                 WHERE v.driver_id IS NOT NULL AND d.driver_id IS NULL""")
            ],
            'riders': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT rider_id) FROM silver.riders"),
                ('email_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT email) FROM silver.riders WHERE email IS NOT NULL")
            ],
            'trips': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT trip_id) FROM silver.trips"),
                ('fk_rider', """SELECT COUNT(*) FROM silver.trips t
                                LEFT JOIN silver.riders r ON t.rider_id = r.rider_id
                                WHERE t.rider_id IS NOT NULL AND r.rider_id IS NULL"""),
                ('fk_driver', """SELECT COUNT(*) FROM silver.trips t
                                 LEFT JOIN silver.drivers d ON t.driver_id = d.driver_id
                                 WHERE t.driver_id IS NOT NULL AND d.driver_id IS NULL"""),
                ('fk_vehicle', """SELECT COUNT(*) FROM silver.trips t
                                  LEFT JOIN silver.vehicles v ON t.vehicle_id = v.vehicle_id
                                  WHERE t.vehicle_id IS NOT NULL AND v.vehicle_id IS NULL""")
            ],
            'payments': [
                ('pk_uniqueness', "SELECT COUNT(*) - COUNT(DISTINCT payment_id) FROM silver.payments"),
                ('fk_trip', """SELECT COUNT(*) FROM silver.payments p
                               LEFT JOIN silver.trips t ON p.trip_id = t.trip_id
                               WHERE p.trip_id IS NOT NULL AND t.trip_id IS NULL""")
            ]
        }

        all_passed = True
        try:
            with engine.begin() as conn:
                for table_name, table_checks in checks.items():
                    logger.info(f"Running DQ checks for {table_name}...")
                    for check_name, sql in table_checks:
                        bad = conn.execute(text(sql)).scalar_one()
                        passed = (bad == 0)
                        if not passed:
                            all_passed = False
                            logger.warning(f"❌ {table_name}.{check_name}: {bad} bad rows")
                        else:
                            logger.info(f"✅ {table_name}.{check_name}: PASSED")
                        conn.execute(text("""
                            INSERT INTO audit.dq_results (table_name, check_name, pass_fail, bad_row_count, run_id)
                            VALUES (:t, :c, :p, :b, :r)
                        """), {"t": table_name, "c": check_name, "p": passed, "b": int(bad), "r": self.run_id})
        except Exception as e:
            logger.error(f"Error running DQ checks: {e}")
            return False

        if all_passed:
            logger.info("✅ All Data Quality checks passed")
        else:
            logger.warning("⚠️  Some Data Quality checks failed — see audit.dq_results")
        return True

    # ---------------- Audit logging helpers ----------------
    def log_etl_step(self, step_name: str, table_name: str,
                     input_count: int | None, output_count: int | None, rejected_count: int | None):
        checksum = None
        try:
            if output_count and output_count > 0:
                checksum = self._calculate_checksum(table_name)
        except Exception as e:
            logger.warning(f"Checksum skipped for {table_name}: {e}")

        try:
            run_sql("""
                INSERT INTO audit.etl_log
                (run_id, run_timestamp, step_executed, table_name, input_row_count,
                 output_row_count, rejected_row_count, data_checksum)
                VALUES (:run_id, :ts, :step, :table, :in_c, :out_c, :rej_c, :chk)
            """, {
                "run_id": self.run_id,
                "ts": datetime.now(),
                "step": step_name,
                "table": table_name,
                "in_c": input_count,
                "out_c": output_count,
                "rej_c": rejected_count,
                "chk": checksum
            })
        except Exception as e:
            logger.error(f"Error logging ETL step ({table_name} - {step_name}): {e}")

    def _calculate_checksum(self, table_name: str):
        # Prefer final table if it exists, otherwise base
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT to_regclass(:tbl) IS NOT NULL"),
                {"tbl": f"silver.{table_name}"}
            ).scalar_one()
            obj = f"silver.{table_name}" if exists else f"silver.{table_name}_base"

            # md5 over first 1000 json rows for reproducible lightweight checksum
            res = conn.execute(text(f"""
                SELECT MD5(STRING_AGG(md5_row, '' ORDER BY md5_row)) AS md5
                FROM (
                    SELECT MD5(CAST(ROW_TO_JSON(t.*) AS TEXT)) AS md5_row
                    FROM (SELECT * FROM {obj} LIMIT 1000) t
                ) s
            """)).scalar_one()
            return res

    # ---------------- Final summary ----------------
    def log_summary(self):
        logger.info("=" * 60)
        logger.info("📊 SILVER LAYER PROCESSING SUMMARY")
        logger.info("=" * 60)

        total_input = sum(stats.get('input_rows', 0) for stats in self.stats.values())
        total_valid = sum(stats.get('valid_rows', 0) for stats in self.stats.values())
        total_invalid = sum(stats.get('invalid_rows', 0) for stats in self.stats.values())

        for table_name in self.tables:
            stats = self.stats.get(table_name, {})
            input_rows = stats.get('input_rows', 0)
            valid_rows = stats.get('valid_rows', 0)
            invalid_rows = stats.get('invalid_rows', 0)
            logger.info(f"  {table_name:<10}: {input_rows:>8,} → {valid_rows:>8,} valid, {invalid_rows:>6,} rejected")

        logger.info("-" * 60)
        logger.info(f"  {'TOTAL':<10}: {total_input:>8,} → {total_valid:>8,} valid, {total_invalid:>6,} rejected")
        logger.info(f"  Run ID: {self.run_id}")
        logger.info("=" * 60)


# ---------------- Orchestration ----------------
def main():
    logger.info("🥈 MEDALLION SILVER LAYER - BUILDER STARTED")

    sb = SilverBuilder()

    if not sb.setup_schemas():
        raise SystemExit(1)

    if not sb.create_silver_base_tables():
        raise SystemExit(1)

    sb.deep_validation()
    sb.run_data_quality_checks()
    sb.log_summary()

    logger.info("🎉 Silver build completed")


if __name__ == "__main__":
    main()
