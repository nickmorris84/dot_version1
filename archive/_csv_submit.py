%load_ext autoreload
%autoreload 2

import requests
import pandas as pd
import io
from core.transforms.ingest import ingest_from_csv
from services.master_workflow import run_workflow

# Create a small DataFrame for testing
# df = pd.DataFrame({
#     "id": [1, 2, 3],
#     "name": ["Alice", "Bob", "Charlie"],
#     "amount": [100, 200, 300]
# })

# # Convert to CSV in memory
# csv_bytes = io.BytesIO()
# df.to_csv(csv_bytes, index=False)
# csv_bytes.seek(0)

RAW_DATA_PATH = "data/raw/credit_card_process_activities.csv"

raw = ingest_from_csv(RAW_DATA_PATH)
csv_bytes = io.BytesIO()
raw.to_csv(csv_bytes, index=False)
csv_bytes.seek(0)

# Send to the Flask API
headers = {"Authorization": "Bearer changeme"}  # if you set CSV_API_TOKEN
files = {"file": ("test.csv", csv_bytes, "text/csv")}
data = {"dedupe": "true", "dropna": "true"}

print(raw.columns)
# print(raw)

ds = run_workflow(data=raw) #<- run this as a proxy
print(f' final output: {ds.df}')

# ds.records