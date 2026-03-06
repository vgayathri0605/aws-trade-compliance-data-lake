import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed")

SANCTIONED_COUNTRIES = ["IRAN", "NORTH KOREA"]
VALID_CURRENCIES = ["USD", "EUR", "GBP", "INR", "CNY"]