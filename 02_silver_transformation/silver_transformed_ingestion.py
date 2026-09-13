import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, current_timestamp
from delta.tables import DeltaTable

def get_clinic_data(spark: SparkSession, base_adls: str):
    print("Extracting and mapping Bronze Clinic data...")
    df_csv = spark.read.format("delta").load(f"{base_adls}/delta/bronze_clinic_claims_csv")
    df_parquet = spark.read.format("delta").load(f"{base_adls}/delta/bronze_clinic_claims_parquet")
    
    df_clinic_raw = df_csv.unionByName(df_parquet, allowMissingColumns=True)
    
    return df_clinic_raw.select(
        col("claim_id").alias("unified_claim_id"),
        col("patient_id"),
        to_date(col("encounter_date"), "yyyy-MM-dd").alias("service_date"),
        col("claim_amount").cast("double").alias("total_amount"),
        lit("CLINIC").alias("source_system")
    )

def get_hospital_data(spark: SparkSession, base_adls: str):
    print("Extracting and mapping Bronze Hospital data...")
    df_hosp_raw = spark.read.format("delta").load(f"{base_adls}/delta/bronze_hospital_claims")
    
    return df_hosp_raw.select(
        col("hospital_claim_id").alias("unified_claim_id"),
        col("patient_id"),
        to_date(col("admission_date"), "yyyy-MM-dd").alias("service_date"),
        col("total_facility_charges").cast("double").alias("total_amount"),
        lit("HOSPITAL").alias("source_system")
    )

def get_edi_837_data(spark: SparkSession, base_adls: str):
    print("Extracting and mapping Bronze EDI-837 data...")
    df_edi_raw = spark.read.format("delta").load(f"{base_adls}/delta/bronze_edi_837_claims")
    
    return df_edi_raw.select(
        col("claim_information.claim_id").alias("unified_claim_id"),
        col("subscriber.patient_id").alias("patient_id"),
        to_date(col("_ingested_at")).alias("service_date"),
        col("claim_information.total_claim_charge_amount").cast("double").alias("total_amount"),
        lit("EDI-837").alias("source_system")
    )

def build_silver_layer(spark: SparkSession, base_adls: str):
    target_path = f"{base_adls}/delta/silver_unified_claims"
    
    df_clinic = get_clinic_data(spark, base_adls)
    df_hospital = get_hospital_data(spark, base_adls)
    df_edi = get_edi_837_data(spark, base_adls)
    
    df_unified = (
        df_clinic
        .unionByName(df_hospital)
        .unionByName(df_edi)
        .withColumn("_silver_processed_at", current_timestamp())
    )
    
    print("Merging data into Silver unified table...")
    if DeltaTable.isDeltaTable(spark, target_path):
        silver_table = DeltaTable.forPath(spark, target_path)
        
        (silver_table.alias("target")
         .merge(
             df_unified.alias("source"),
             "target.unified_claim_id = source.unified_claim_id AND target.source_system = source.source_system"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print(f"Merge successful. Target: {target_path}")
    else:
        print("Silver table does not exist. Creating new Delta table...")
        df_unified.write.format("delta").mode("overwrite").save(target_path)
        print(f"Table created successfully. Target: {target_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_account", default="lshc", help="Azure Storage Account name")
    parser.add_argument("--container", default="landing-zone", help="ADLS Container name")
    args, unknown = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    base_adls = f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"

    build_silver_layer(spark, base_adls)