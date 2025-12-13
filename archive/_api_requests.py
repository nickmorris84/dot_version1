%load_ext autoreload
%autoreload 2

import requests
import pandas as pd
import io
from core.transforms.ingest import ingest_from_csv
# from pipelines.master_workflow import run_workflow  # optional if used

# Load the CSV file into a DataFrame
RAW_DATA_PATH = "data/raw/credit_card_process_activities.csv"
raw = ingest_from_csv(RAW_DATA_PATH)

# Preview
print("Sending columns:", list(raw.columns))

# Convert DataFrame to in-memory CSV
csv_bytes = io.BytesIO()
raw.to_csv(csv_bytes, index=False)
csv_bytes.seek(0)

# --- Prepare request ---
url = "http://0.0.0.0:8000/ingest_csv"

headers = {
    "x-api-key": "changeme"  # or whatever your API_KEY is
}

files = {
    "file": ("credit_card_data.csv", csv_bytes, "text/csv")
}

# Form fields (match FastAPI Form parameters)
data = {
    "run_mode": "sync",
    "save": "false"
}

# Send POST request
response = requests.post(url, headers=headers, files=files, data=data)

# Print response
try:
    response.raise_for_status()
    resp_json = response.json()
    print("✅ Response:", resp_json)
except requests.HTTPError as e:
    print("❌ HTTP error:", e)
    print("Response body:", response.text)