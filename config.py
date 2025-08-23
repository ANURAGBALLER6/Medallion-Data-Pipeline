import os

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'medallion_architecture'),
    'user': os.getenv('DB_USER', 'anusix'),
    'password': os.getenv('DB_PASSWORD', 'Anurag@123'),
    'port': int(os.getenv('DB_PORT', '5432'))
}

# Google Sheets Configuration
GOOGLE_SHEETS_CONFIG = {
    'credentials_path': os.getenv('GOOGLE_CREDS_PATH', '/home/nineleaps/Downloads/medallion-469815-fca267526bda.json'),
    'scopes': ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    'spreadsheet_id': os.getenv('SPREADSHEET_ID', '1RdMm7Z_szLYAmoV79LJwfQJa2w5vG76eMTWGxk7egLg')
}

# Sheet Ranges for different data types
SHEET_RANGES = {
    'drivers': 'drivers!A:I',
    'vehicles': 'vehicles!A:J',
    'riders': 'riders!A:H',
    'trips': 'trips!A:R',
    'payments': 'payments!A:H'
}

# Logging Configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(levelname)s - %(message)s',
    'log_dir': 'logs'
}