🚖 Trips & Drivers Data Pipeline

This project implements a Medallion Architecture (Bronze → Silver → Gold) for processing and analyzing data from a ride-hailing platform (drivers, trips, vehicles, payments, ratings).

The pipeline uses PostgreSQL as the database backend and will evolve into a full ETL system with data governance, quality checks, and analytics dashboards.

📌 Day 1 Goals

✅ Initialize project structure in PyCharm

✅ Setup Git repository

✅ Connect PostgreSQL database

✅ Plan Bronze layer schema (drivers, trips, vehicles, payments, ratings)

🚧 Start ingestion scripts (Day 2)

📁 Project Structure (Initial)
Trips-Data-Pipeline/
├── bronze/                 # Raw ingestion layer
├── silver/                 # Clean & validated data
├── gold/                   # Analytics (future)
├── logs/                   # Execution logs
├── config.py               # DB configuration
├── etl.py                  # Main pipeline orchestration
└── README.md               # Documentation

🏗️ Architecture Overview

Data Flow:

Source Data (CSV/API) → Bronze (Raw Storage) → Silver (Validated Data) → Gold (Analytics)
                                ↓                      ↓
                          Logs & Audit            Quality Checks


Bronze → Store raw drivers, vehicles, trips, payments, ratings

Silver → Deduplicate, validate (emails, phone, timestamps, fares)

Gold → Build KPIs (driver earnings, trip volume, customer satisfaction)

⚡ Setup Instructions
1. Prerequisites

Install PostgreSQL

Install Python 3.10+

Create virtual environment:

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

2. Database Setup

Start PostgreSQL and create a database:

sudo -u postgres psql -c "CREATE DATABASE trips_pipeline;"
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'password123';"


Update config.py:

DB_CONFIG = {
    'host': 'localhost',
    'database': 'trips_pipeline',
    'user': 'postgres',
    'password': 'password123',
    'port': 5432
}

🚀 Roadmap
Day 1 (Today)

Setup project & repo

Database connection ready

Bronze schema planning

Day 2

Ingest raw data into Bronze

Create base tables for drivers, vehicles, trips

Day 3

Silver layer validations (emails, phone, fares, timestamps)

Day 4+

Gold analytics (trip metrics, driver KPIs, revenue dashboards)

📄 License

This project is for educational and enterprise data pipeline development purposes.

👉 Would you like me to make the README daily-progress style (Day 1, Day 2, … like a journal), or a single professional README that evolves as you code?

ChatGPT can make mistakes. Check important info. See Cookie Preferences.