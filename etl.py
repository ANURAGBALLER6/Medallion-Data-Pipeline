"""
Main ETL orchestration for Medallion Data Pipeline
Supports Bronze -> Silver -> Gold transformations
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path
from datetime import datetime

# -----------------------------------------------------------------------------
# Load environment variables
# -----------------------------------------------------------------------------
load_dotenv()

# Add project root to sys.path
ROOT = Path(__file__).parent
sys.path.append(str(ROOT))

# Config
from config import LOG_CONFIG
from silver.silver_builder import SilverBuilder
from gold.gold import GoldBuilder

# -----------------------------------------------------------------------------
# Local DB Engine
# -----------------------------------------------------------------------------
LOCAL_DB = os.getenv("LOCAL_DB")

if not LOCAL_DB:
    print("❌ LOCAL_DB not found in environment variables. Please set it in .env")
    sys.exit(1)

local_engine = create_engine(LOCAL_DB, future=True)

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------
log_dir = ROOT / LOG_CONFIG["log_dir"]
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_CONFIG["level"]),
    format=LOG_CONFIG["format"],
    handlers=[
        logging.FileHandler(log_dir / "etl.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("etl")


# -----------------------------------------------------------------------------
# Bronze Layer
# -----------------------------------------------------------------------------
def build_bronze() -> bool:
    """Build Bronze layer - Extract and Load raw data."""
    logger.info("🥉 Building Bronze Layer...")
    try:
        from bronze.data_loader import load_all_data_to_bronze
        success = load_all_data_to_bronze()
        if success:
            logger.info("✅ Bronze layer built successfully")
            return True
        else:
            logger.error("❌ Bronze layer build failed")
            return False
    except Exception as e:
        logger.exception(f"❌ Error building Bronze layer: {e}")
        return False


# -----------------------------------------------------------------------------
# Silver Layer
# -----------------------------------------------------------------------------
def build_silver() -> bool:
    """Build Silver layer - Transform and validate data."""
    logger.info("🥈 Building Silver Layer...")
    try:
        silver_builder = SilverBuilder()

        # Step 1: Setup schemas and audit tables
        logger.info("Step 1: Setting up Silver and Audit schemas...")
        if not silver_builder.setup_schemas():
            logger.error("Failed to setup schemas")
            return False

        # Step 2: Light cleaning in SQL (Bronze -> Silver base)
        logger.info("Step 2: Performing light cleaning in SQL...")
        if not silver_builder.create_silver_base_tables():
            logger.error("Failed to create Silver base tables")
            return False

        # Step 3: Deep validation in Python
        logger.info("Step 3: Performing deep validation in Python...")
        if not silver_builder.deep_validation():
            logger.error("Failed to perform deep validation")
            return False

        # Step 4: Data Quality checks
        logger.info("Step 4: Running Data Quality checks...")
        if not silver_builder.run_data_quality_checks():
            logger.error("Failed to run DQ checks")
            return False

        silver_builder.log_summary()
        logger.info("✅ Silver layer built successfully")
        return True

    except Exception as e:
        logger.exception(f"❌ Error building Silver layer: {e}")
        return False


# -----------------------------------------------------------------------------
# Gold Layer
# -----------------------------------------------------------------------------
from gold.gold import GoldBuilder, export_gold_to_csv, engine

def build_gold() -> bool:
    logger.info("🥇 Building Gold Layer...")
    try:
        gold_builder = GoldBuilder()
        success = gold_builder.run()

        if success:
            logger.info("✅ Gold layer built successfully")

            output_dir = ROOT / "gold"
            output_dir.mkdir(exist_ok=True)

            # ✅ Use the global engine
            with engine.connect() as conn:
                export_gold_to_csv(conn, output_dir)

            logger.info(f"📂 Gold tables exported to CSV in {output_dir}")
            return True
        else:
            logger.error("❌ Gold layer build failed")
            return False
    except Exception as e:
        logger.exception(f"❌ Error building Gold layer: {e}")
        return False


# -----------------------------------------------------------------------------
# Pipeline Orchestration
# -----------------------------------------------------------------------------
def run_full_pipeline() -> bool:
    """Run the complete ETL pipeline: Bronze -> Silver -> Gold."""
    start_time = datetime.now()
    logger.info("🚀 STARTING FULL MEDALLION ETL PIPELINE")
    logger.info("=" * 60)

    results = {"bronze": False, "silver": False, "gold": False}

    # Bronze
    results["bronze"] = build_bronze()

    # Silver (only if Bronze succeeds)
    if results["bronze"]:
        results["silver"] = build_silver()
    else:
        logger.warning("⚠️  Skipping Silver layer due to Bronze failures")

    # Gold (only if Silver succeeds)
    if results["silver"]:
        results["gold"] = build_gold()
    else:
        logger.warning("⚠️  Skipping Gold layer due to Silver failures")

    # Summary
    end_time = datetime.now()
    duration = end_time - start_time

    logger.info("=" * 60)
    logger.info("📊 ETL PIPELINE SUMMARY")
    logger.info("=" * 60)
    for layer, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        logger.info(f"  {layer.upper():<8}: {status}")

    successful_layers = sum(results.values())
    total_layers = len(results)

    logger.info(
        f"\n📈 Overall: {successful_layers}/{total_layers} layers completed successfully"
    )
    logger.info(f"⏱️  Total duration: {duration}")

    if successful_layers == total_layers:
        logger.info("🎉 Full pipeline completed successfully!")
        return True
    else:
        logger.warning("⚠️  Pipeline completed with some failures")
        return False


# -----------------------------------------------------------------------------
# CLI Entry
# -----------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Medallion Data Pipeline ETL")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which layer to build",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild even if layer exists",
    )

    args = parser.parse_args()
    logger.info(f"🎯 Target layer: {args.layer}")
    if args.force:
        logger.info("🔄 Force rebuild enabled")

    success = False
    if args.layer == "bronze":
        success = build_bronze()
    elif args.layer == "silver":
        success = build_silver()
    elif args.layer == "gold":
        success = build_gold()
    elif args.layer == "all":
        success = run_full_pipeline()

    if success:
        logger.info("✅ ETL process completed successfully")
        sys.exit(0)
    else:
        logger.error("❌ ETL process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
#