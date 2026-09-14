import io
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
from azure.storage.blob import BlobServiceClient

# --- AZURE CONFIGURATION ---
STORAGE_ACCOUNT = "<storage account name"
CONTAINER_NAME = "<azure container name"
STORAGE_KEY = "<your storage key here"
CONNECTION_STRING = (
    f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};"
    f"AccountKey={STORAGE_KEY};EndpointSuffix=core.windows.net"
)

def generate_clinic_data(n_records=500):
    diagnoses = ["J01.90", "E11.9", "I10", "M54.5", "J45.909", "N39.0"]
    
    records = []
    base_date = datetime.now() - timedelta(days=30)
    
    for _ in range(n_records):
        records.append({
            "claim_id": f"CLN-{uuid.uuid4().hex[:8].upper()}",
            "patient_id": f"PAT-{random.randint(1000, 9999)}",
            "doctor_id": f"DOC-{random.randint(10, 99)}",
            "encounter_date": (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d"),
            "diagnosis_code": random.choice(diagnoses),
            "claim_amount": round(random.uniform(75.0, 600.0), 2)
        })
    return pd.DataFrame(records)

if __name__ == "__main__":
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # 1. Generate unique timestamp and new data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_csv = generate_clinic_data(250)
    df_parquet = generate_clinic_data(250)

    # 2. Upload CSV
    csv_buffer = io.StringIO()
    df_csv.to_csv(csv_buffer, index=False)
    csv_path = f"clinic/clinic_claims_{timestamp}.csv"
    container_client.get_blob_client(csv_path).upload_blob(csv_buffer.getvalue().encode("utf-8"), overwrite=True)
    print(f"Uploaded new CSV file: {csv_path}")

    # 3. Upload Parquet
    parquet_buffer = io.BytesIO()
    df_parquet.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    parquet_path = f"clinic/clinic_claims_{timestamp}.parquet"
    container_client.get_blob_client(parquet_path).upload_blob(parquet_buffer.getvalue(), overwrite=True)
    print(f"Uploaded new Parquet file: {parquet_path}")