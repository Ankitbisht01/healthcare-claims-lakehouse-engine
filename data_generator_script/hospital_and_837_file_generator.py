import io
import json
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

def generate_hospital_data(n_records=500):
    drg_codes = ["DRG-470", "DRG-871", "DRG-291", "DRG-392", "DRG-194"]
    facilities = ["FAC-HOSP-01", "FAC-HOSP-02", "FAC-METRO-EAST"]
    
    records = []
    base_date = datetime.now() - timedelta(days=60)
    
    for _ in range(n_records):
        admission = base_date + timedelta(days=random.randint(0, 50))
        los_days = random.randint(1, 14)
        
        records.append({
            "hospital_claim_id": f"HOSP-{uuid.uuid4().hex[:8].upper()}",
            "patient_id": f"PAT-{random.randint(1000, 9999)}",
            "facility_id": random.choice(facilities),
            "admission_date": admission.strftime("%Y-%m-%d"),
            "discharge_date": (admission + timedelta(days=los_days)).strftime("%Y-%m-%d"),
            "drg_code": random.choice(drg_codes),
            "total_facility_charges": round(random.uniform(5000.0, 75000.0), 2)
        })
    return pd.DataFrame(records)

def generate_edi_837_data(n_records=250):
    diagnoses = ["I10", "E11.9", "J44.1", "M54.5"]
    
    edi_claims = []
    for i in range(n_records):
        edi_claims.append({
            "transaction_type": "837P",
            "billing_provider": {
                "npi": f"1{random.randint(100000000, 999999999)}",
                "organization_name": "Apex Health Partners"
            },
            "subscriber": {
                "member_id": f"SUB-{random.randint(10000, 99999)}",
                "patient_id": f"PAT-{random.randint(1000, 9999)}"
            },
            "claim_information": {
                "claim_id": f"EDI837-{uuid.uuid4().hex[:8].upper()}",
                "total_claim_charge_amount": round(random.uniform(150.0, 3200.0), 2),
                "diagnosis_codes": random.sample(diagnoses, k=random.randint(1, 3)),
                "service_lines": [
                    {
                        "line_number": line_idx + 1,
                        "procedure_code": random.choice(["99213", "99214", "33533"]),
                        "unit_price": round(random.uniform(50.0, 500.0), 2)
                    } for line_idx in range(random.randint(1, 4))
                ]
            }
        })
    return edi_claims

if __name__ == "__main__":
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)
    
    # 1. Generate unique timestamp and new data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_hosp = generate_hospital_data(500)
    edi_records = generate_edi_837_data(250)

    # 2. Upload Hospital Parquet
    parquet_buffer = io.BytesIO()
    df_hosp.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    hosp_path = f"hospital/hospital_claims_{timestamp}.parquet"
    container_client.get_blob_client(hosp_path).upload_blob(parquet_buffer.getvalue(), overwrite=True)
    print(f"Uploaded new Hospital file: {hosp_path}")

    # 3. Upload EDI-837 NDJSON
    ndjson_content = "\n".join([json.dumps(r) for r in edi_records])
    edi_path = f"edi_837/edi_837_{timestamp}.json"
    container_client.get_blob_client(edi_path).upload_blob(ndjson_content.encode("utf-8"), overwrite=True)
    print(f"Uploaded new EDI-837 file: {edi_path}")