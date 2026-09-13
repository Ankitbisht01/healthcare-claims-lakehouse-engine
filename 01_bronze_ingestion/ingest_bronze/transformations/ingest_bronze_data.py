from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# --- 1. METADATA REGISTRY ---
SOURCE_CONFIG = {
    "clinic_csv": {
        "paths": ["clinic/"], 
        "format": "csv",
        "target_table": "delta/bronze_clinic_claims_csv",
        "checkpoint": "checkpoints/bronze_clinic_csv",
        "schema": "schema/bronze_clinic_csv",
        "options": {
            "header": "true", 
            "cloudFiles.inferColumnTypes": "true",
            "pathGlobFilter": "*.csv"  # Only read CSV files from this folder
        }
    },
    "clinic_parquet": {
        "paths": ["clinic/"], 
        "format": "parquet",
        "target_table": "delta/bronze_clinic_claims_parquet",
        "checkpoint": "checkpoints/bronze_clinic_parquet",
        "schema": "schema/bronze_clinic_parquet",
        "options": {
            "pathGlobFilter": "*.parquet"  # Only read Parquet files from this folder
        }
    },
    "hospital": {
        "paths": ["hospital/"],
        "format": "parquet",
        "target_table": "delta/bronze_hospital_claims",
        "checkpoint": "checkpoints/bronze_hospital",
        "schema": "schema/bronze_hospital",
        "options": {}
    },
    "edi_837": {
        "paths": ["edi_837/"],
        "format": "json",
        "target_table": "delta/bronze_edi_837_claims",
        "checkpoint": "checkpoints/bronze_edi_837",
        "schema": "schema/bronze_edi_837",
        "options": {
            "multiline": "false", 
            "cloudFiles.inferColumnTypes": "true"
        }
    }
}

BASE_ADLS = "abfss://landing-zone@lshc.dfs.core.windows.net"

# --- 2. INGESTION ENGINE ---
def read_feed(feed_name: str):
    config = SOURCE_CONFIG[feed_name]
    source_path = f"{BASE_ADLS}/{config['paths'][0]}"

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config["format"])
    )

    for k, v in config["options"].items():
        reader = reader.option(k, v)

    return (
        reader.load(source_path)
        .withColumn("_source_feed", lit(feed_name))
        .withColumn("_ingested_file_name", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )


@dp.table(name="bronze_clinic_claims_csv")
def bronze_clinic_claims_csv():
    return read_feed("clinic_csv")


@dp.table(name="bronze_clinic_claims_parquet")
def bronze_clinic_claims_parquet():
    return read_feed("clinic_parquet")


@dp.table(name="bronze_hospital_claims")
def bronze_hospital_claims():
    return read_feed("hospital")


@dp.table(name="bronze_edi_837_claims")
def bronze_edi_837_claims():
    return read_feed("edi_837")