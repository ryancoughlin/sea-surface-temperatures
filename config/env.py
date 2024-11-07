import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if it exists
env_path = Path('.') / '.env'
load_dotenv(env_path)

# CMEMS credentials
CMEMS_USERNAME = os.getenv('CMEMS_USERNAME')
CMEMS_PASSWORD = os.getenv('CMEMS_PASSWORD')

if not all([CMEMS_USERNAME, CMEMS_PASSWORD]):
    raise ValueError(
        "Missing CMEMS credentials. Please set CMEMS_USERNAME and CMEMS_PASSWORD "
        "environment variables or add them to .env file"
    ) 