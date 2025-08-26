#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold Layer Builder for Medallion Data Pipeline

Builds BI-ready aggregates and a flattened dashboard table on top of Silver.
Includes lightweight reconciliation checks against Silver with tolerance
logging into audit tables.

Outputs
-------
Schema: gold
Tables:
  - gold.driver_stats
  - gold.vehicle_stats
  - gold.rider_stats
  - gold.daily_kpis
  - gold.dashboard
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from urllib.parse import quote_plus
import psycopg2

def get_connection():
    """Centralized DB connection for GoldBuilder"""
    return psycopg2.connect(
        dbname="medallion_architecture",
        user="anusix",
        password="Anurag@123",
        host="localhost",
        port=5432
    )

# ---------------- Config import ----------------
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG, LOG_CONFIG  # noqa: E402

# ---------------- Logging ----------------
log_dir = Path(__file__).parent.parent / LOG_CONFIG.get("log_dir", "logs")
log_dir.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG.get("level", "INFO")),
    format=LOG_CONFIG.get("format", "%(asctime)s - %(levelname)s - %(message)s"),
    handlers=[
        logging.FileHandler(log_dir / "gold_builder.log"),
        logging.StreamHandler(),
    ],
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
    return create_engine(url, future=True)


engine = make_engine()


def run_sql(sql: str, params: Optional[dict] = None):
    """Run a single SQL statement in its own transaction."""
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


class GoldBuilder:
    TOLERANCE = 1e-6

    def __init__(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    # --------------- Step 1: Schemas + audit helpers ---------------
    def setup_schema(self) -> bool:
        logger.info("Setting up Gold schema and audit tables...")
        try:
            run_sql("CREATE SCHEMA IF NOT EXISTS gold;")
            run_sql(
                """
                CREATE TABLE IF NOT EXISTS audit.recon_results (
                    id SERIAL PRIMARY KEY,
                    run_id VARCHAR(50) NOT NULL,
                    check_name VARCHAR(200) NOT NULL,
                    lhs_value NUMERIC,
                    rhs_value NUMERIC,
                    diff NUMERIC,
                    within_tolerance BOOLEAN NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            logger.info("✅ Schemas and audit tables ready (gold, audit.recon_results)")
            return True
        except Exception as e:
            logger.error(f"❌ Error setting up schema/audit: {e}")
            return False

    # --------------- Step 2: Aggregates ---------------
    def build_aggregates(self) -> bool:
        logger.info("Building Gold aggregates...")
        try:
            # Driver Stats (with global averages via CTE)
            run_sql(
                """
                DROP TABLE IF EXISTS gold.driver_stats;
                CREATE TABLE gold.driver_stats AS
                WITH global_avgs AS (
                    SELECT 
                        AVG(tip_usd) AS global_avg_tip_usd
                    FROM silver.trips
                    WHERE tip_usd IS NOT NULL
                )
                SELECT 
                    d.driver_id,
                    COUNT(t.trip_id) AS total_trips,
                    COALESCE(SUM(p.amount_usd), 0) AS total_earnings_usd,
                    AVG(t.total_fare_usd) FILTER (WHERE t.total_fare_usd IS NOT NULL) AS avg_trip_fare_usd,
                    AVG(t.tip_usd) FILTER (WHERE t.tip_usd IS NOT NULL) AS avg_tip_usd,
                    AVG(t.tip_usd / NULLIF(t.total_fare_usd,0)) FILTER (WHERE t.total_fare_usd IS NOT NULL) AS avg_tip_rate,
                    AVG((t.tip_usd > 0)::INT)::NUMERIC AS tip_take_rate,
                    g.global_avg_tip_usd
                FROM silver.drivers d
                LEFT JOIN silver.trips t ON t.driver_id = d.driver_id
                LEFT JOIN silver.payments p ON p.trip_id = t.trip_id
                CROSS JOIN global_avgs g
                GROUP BY d.driver_id, g.global_avg_tip_usd;
                """
            )

            # Vehicle Stats
            run_sql(
                """
                DROP TABLE IF EXISTS gold.vehicle_stats;
                CREATE TABLE gold.vehicle_stats AS
                SELECT 
                    v.vehicle_id,
                    v.driver_id,
                    COUNT(t.trip_id) AS total_trips,
                    COALESCE(SUM(p.amount_usd), 0) AS total_revenue_usd,
                    AVG(t.duration_min) AS avg_duration_min,
                    AVG(t.distance_km) AS avg_distance_km
                FROM silver.vehicles v
                LEFT JOIN silver.trips t ON t.vehicle_id = v.vehicle_id
                LEFT JOIN silver.payments p ON p.trip_id = t.trip_id
                GROUP BY v.vehicle_id, v.driver_id;
                """
            )

            # Rider Stats
            run_sql(
                """
                DROP TABLE IF EXISTS gold.rider_stats;
                CREATE TABLE gold.rider_stats AS
                SELECT 
                    r.rider_id,
                    COUNT(t.trip_id) AS total_trips,
                    COALESCE(SUM(p.amount_usd), 0) AS total_spend_usd,
                    AVG(t.total_fare_usd) FILTER (WHERE t.total_fare_usd IS NOT NULL) AS avg_trip_fare_usd,
                    MIN(t.request_ts)::DATE AS first_trip_date,
                    MAX(t.request_ts)::DATE AS last_trip_date
                FROM silver.riders r
                LEFT JOIN silver.trips t ON t.rider_id = r.rider_id
                LEFT JOIN silver.payments p ON p.trip_id = t.trip_id
                GROUP BY r.rider_id;
                """
            )

            # Daily KPIs
            run_sql(
                """
                DROP TABLE IF EXISTS gold.daily_kpis;
                CREATE TABLE gold.daily_kpis AS
                WITH trip_dates AS (
                    SELECT 
                        t.trip_id,
                        COALESCE(t.dropoff_ts, t.pickup_ts, t.request_ts)::DATE AS trip_date,
                        t.driver_id,
                        t.rider_id
                    FROM silver.trips t
                )
                SELECT 
                    td.trip_date,
                    COUNT(td.trip_id) AS trips,
                    COUNT(DISTINCT td.driver_id) AS active_drivers,
                    COUNT(DISTINCT td.rider_id) AS active_riders,
                    COALESCE(SUM(p.amount_usd), 0) AS total_revenue_usd,
                    AVG(p.amount_usd) FILTER (WHERE p.amount_usd IS NOT NULL) AS avg_revenue_per_trip_usd
                FROM trip_dates td
                LEFT JOIN silver.payments p ON p.trip_id = td.trip_id
                GROUP BY td.trip_date
                ORDER BY td.trip_date;
                """
            )

            logger.info("✅ Aggregates created (driver_stats, vehicle_stats, rider_stats, daily_kpis)")
            return True
        except Exception as e:
            logger.error(f"❌ Error building aggregates: {e}")
            return False

    # --------------- Step 3: Dashboard table ---------------
    def build_dashboard(self) -> bool:
        logger.info("Building Gold dashboard table...")
        try:
            # Ensure one row per trip → pre-aggregate payments
            run_sql(
                """
                DROP TABLE IF EXISTS gold.dashboard;
                CREATE TABLE gold.dashboard AS
                WITH pay_agg AS (
                    SELECT trip_id,
                           SUM(amount_usd) AS fare_usd,
                           SUM(tip_usd)   AS tip_usd_payment,
                           MAX(payment_method) AS payment_method,
                           MAX(status) AS payment_status
                    FROM silver.payments
                    GROUP BY trip_id
                )
                SELECT 
                    t.trip_id,
                    COALESCE(t.dropoff_ts, t.pickup_ts, t.request_ts)::DATE AS trip_date,
                    t.request_ts, t.pickup_ts, t.dropoff_ts,
                    t.driver_id,
                    t.vehicle_id,
                    t.rider_id,
                    t.pickup_location,
                    t.drop_location,
                    t.distance_km,
                    t.duration_min,
                    t.wait_time_minutes,
                    t.surge_multiplier,
                    t.base_fare_usd,
                    t.tax_usd,
                    t.tip_usd,
                    t.total_fare_usd,
                    p.payment_method,
                    p.fare_usd,
                    p.tip_usd_payment,
                    p.payment_status,
                    v.make, v.model, v.year, v.capacity, v.color,
                    d.city AS driver_city
                FROM silver.trips t
                LEFT JOIN pay_agg p ON p.trip_id = t.trip_id
                LEFT JOIN silver.vehicles v ON v.vehicle_id = t.vehicle_id
                LEFT JOIN silver.drivers d  ON d.driver_id  = t.driver_id;
                """
            )
            logger.info("✅ gold.dashboard created")
            return True
        except Exception as e:
            logger.error(f"❌ Error building dashboard: {e}")
            return False

    # --------------- Step 4: Reconciliation ---------------
    def reconcile(self) -> bool:
        logger.info("Running reconciliation checks...")
        ok = True

        checks = [
            {
                "name": "trips_count_vs_dashboard_count",
                "silver_sql": "SELECT COUNT(*) FROM silver.trips",
                "gold_sql": "SELECT COUNT(*) FROM gold.dashboard",
                "tolerance": 0
            },
            {
                "name": "tips_sum_vs_dashboard_sum",
                "silver_sql": "SELECT COALESCE(SUM(tip_usd),0) FROM silver.trips",
                "gold_sql": "SELECT COALESCE(SUM(tip_usd),0) FROM gold.dashboard",
                "tolerance": 0.01
            },
            {
                "name": "drivers_count_vs_driver_stats",
                "silver_sql": "SELECT COUNT(DISTINCT driver_id) FROM silver.drivers",
                "gold_sql": "SELECT COUNT(DISTINCT driver_id) FROM gold.driver_stats",
                "tolerance": 0
            },
            {
                "name": "riders_count_vs_rider_stats",
                "silver_sql": "SELECT COUNT(DISTINCT rider_id) FROM silver.riders",
                "gold_sql": "SELECT COUNT(DISTINCT rider_id) FROM gold.rider_stats",
                "tolerance": 0
            },
        ]

        with engine.begin() as conn:
            for check in checks:
                try:
                    lhs = conn.execute(text(check["silver_sql"])).scalar() or 0
                    rhs = conn.execute(text(check["gold_sql"])).scalar() or 0
                    diff = rhs - lhs
                    within = abs(diff) <= check["tolerance"]

                    conn.execute(
                        text(
                            """
                            INSERT INTO audit.recon_results
                            (run_id, check_name, lhs_value, rhs_value, diff, within_tolerance)
                            VALUES (:r, :c, :l, :h, :d, :w)
                            """
                        ),
                        {
                            "r": self.run_id,
                            "c": check["name"],
                            "l": lhs,
                            "h": rhs,
                            "d": diff,
                            "w": within,
                        },
                    )

                    if within:
                        logger.info(f"✅ {check['name']}: OK (diff={diff:.6f})")
                    else:
                        logger.warning(f"⚠️ {check['name']}: OUT OF TOLERANCE (diff={diff:.6f})")
                        ok = False
                except Exception as e:
                    logger.error(f"❌ Error during reconciliation for {check['name']}: {e}")
                    ok = False

        return ok

    # --------------- Helpers ---------------
    def _record_recon(self, conn, check_name: str, lhs: float, rhs: float):
        diff = float(lhs) - float(rhs)
        within = abs(diff) <= self.TOLERANCE
        conn.execute(
            text(
                """
                INSERT INTO audit.recon_results
                (run_id, check_name, lhs_value, rhs_value, diff, within_tolerance)
                VALUES (:r, :c, :l, :h, :d, :w)
                """
            ),
            {"r": self.run_id, "c": check_name, "l": lhs, "h": rhs, "d": diff, "w": within},
        )
        if within:
            logger.info(f"✅ {check_name}: OK (diff={diff:.6f})")
        else:
            logger.warning(f"⚠️  {check_name}: OUT OF TOLERANCE (diff={diff:.6f})")

    # --------------- Orchestration ---------------
    def run(self) -> bool:
        logger.info("🥇 MEDALLION GOLD LAYER - BUILDER STARTED")
        if not self.setup_schema():
            return False
        ok = self.build_aggregates()
        ok = self.build_dashboard() and ok
        ok = self.reconcile() and ok

        self.show_reconciliation_summary()

        if ok:
            logger.info("🎉 Gold build completed successfully")
        else:
            logger.warning("⚠️  Gold build completed with warnings/errors")
        return ok

    def show_reconciliation_summary(self):
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT check_name, within_tolerance, diff
                FROM audit.recon_results
                WHERE run_id = :r
                ORDER BY check_name;
            """), {"r": self.run_id}).fetchall()

        logger.info("📊 Reconciliation Summary:")
        for check_name, within, diff in rows:
            status = "OK" if within else "OUT OF TOLERANCE"
            logger.info(f" - {check_name}: {status} (diff={diff})")

    def run(self) -> bool:
        logger.info("🥇 MEDALLION GOLD LAYER - BUILDER STARTED")
        if not self.setup_schema():
            return False
        ok = self.build_aggregates()
        ok = self.build_dashboard() and ok
        ok = self.reconcile() and ok

        self.show_reconciliation_summary()

        if ok:
            logger.info("🎉 Gold build completed successfully")
        else:
            logger.warning("⚠️  Gold build completed with warnings/errors")
        return ok


def main():
    gb = GoldBuilder()
    gb.run()


if __name__ == "__main__":
    main()