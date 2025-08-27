from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import logging

# ---------------------------
# 🚀 Run ETL Function
# ---------------------------
def run_pipeline():
    logging.info("🚀 Starting Medallion ETL Pipeline...")
    try:
        result = subprocess.run(["python3", "etl.py"], check=True, capture_output=True, text=True)
        logging.info("✅ Pipeline finished successfully.")
        logging.info(f"Logs:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logging.error("❌ Pipeline failed.")
        logging.error(f"Error:\n{e.stderr}")

# ---------------------------
# 📅 Scheduler Setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

scheduler = BlockingScheduler()

# Run every day at 1 PM (13:00 UTC or system timezone)
scheduler.add_job(run_pipeline, 'cron', hour=22, minute=0)

# ✅ Run immediately once (for testing right now)
run_pipeline()

try:
    logging.info("📅 Scheduler started. Waiting for jobs...")
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    logging.info("🛑 Scheduler stopped.")
