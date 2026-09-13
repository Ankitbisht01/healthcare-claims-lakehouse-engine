import argparse
from pyspark.sql import SparkSession
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

# --- 2. INGESTION ENGINE ---
def ingest_feed(spark: SparkSession, base_adls: str, feed_name: str, config: dict):
    source_path = f"{base_adls}/{config['paths'][0]}"
    target_path = f"{base_adls}/{config['target_table']}"
    checkpoint_path = f"{base_adls}/{config['checkpoint']}"
    schema_path = f"{base_adls}/{config['schema']}"

    print(f"==================================================")
    print(f"Starting ingestion for: {feed_name}")
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")
    
    # Configure Auto Loader Reader
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", config["format"])
        .option("cloudFiles.schemaLocation", schema_path)
    )

    # Apply custom options (like headers, glob filters, schema inference)
    for k, v in config["options"].items():
        reader = reader.option(k, v)

    # Read the stream and append audit columns
    stream_df = (
        reader.load(source_path)
        .withColumn("_source_feed", lit(feed_name))
        .withColumn("_ingested_file_name", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
    )

    # Write incrementally to Delta Lake
    query = (
        stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # Processes new files in a single batch, then shuts down
        .start(target_path)
    )
    
    query.awaitTermination()
    print(f"Successfully finished ingestion for: {feed_name}")
    print(f"==================================================\n")

# --- 3. ENTRY POINT ---
if __name__ == "__main__":
    # argparse configured with default values to run directly in the Databricks UI
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_account", default="lshc", help="Azure Storage Account name")
    parser.add_argument("--container", default="landing-zone", help="ADLS Container name")
    parser.add_argument("--feed", default="all", choices=["all", "clinic_csv", "clinic_parquet", "hospital", "edi_837"])
    
    # parse_known_args is safer than parse_args in Databricks environments
    args, unknown = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    base_adls = f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"

    # Execute based on the requested feed
    if args.feed == "all":
        for feed_name, config in SOURCE_CONFIG.items():
            ingest_feed(spark, base_adls, feed_name, config)
    else:
        if args.feed in SOURCE_CONFIG:
            ingest_feed(spark, base_adls, args.feed, SOURCE_CONFIG[args.feed])
        else:
            print(f"Warning: Feed '{args.feed}' is not recognized.")