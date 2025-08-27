import os
from dotenv import load_dotenv
import psycopg2

# load .env
load_dotenv()

# Read env vars
host = os.getenv("SUPABASE_HOST")
port = os.getenv("SUPABASE_PORT")
dbname = os.getenv("SUPABASE_DB")
user = os.getenv("SUPABASE_USER")
password = os.getenv("SUPABASE_PASSWORD")

print("🔑 Using connection params:")
print(f"host={host}, port={port}, db={dbname}, user={user}")

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password  # passing password separately avoids URL parsing issues
    )
    print("✅ Connection successful!")
    conn.close()
except Exception as e:
    print("❌ Connection failed:", e)
