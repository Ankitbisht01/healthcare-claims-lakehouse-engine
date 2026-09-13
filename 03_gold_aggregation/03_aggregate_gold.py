import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, max, min, current_timestamp

def build_financial_summary(silver_df, base_adls):
    print("Building Gold: Daily Financial Summary...")
    target_path = f"{base_adls}/delta/gold_financial_summary_daily"
    
    # Aggregate data by date and source system
    df_financial = (
        silver_df.groupBy("service_date", "source_system")
        .agg(
            count("unified_claim_id").alias("total_claim_count"),
            sum("total_amount").alias("total_revenue"),
            max("total_amount").alias("max_single_claim_amount")
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )
    
    # Gold aggregated tables are often completely overwritten (refreshed) 
    # each run to provide a perfect snapshot for BI tools.
    df_financial.write.format("delta").mode("overwrite").save(target_path)
    print(f"Successfully refreshed: {target_path}")

def build_patient_summary(silver_df, base_adls):
    print("Building Gold: Patient Lifetime Summary...")
    target_path = f"{base_adls}/delta/gold_patient_summary"
    
    # Aggregate data at the patient level
    df_patient = (
        silver_df.groupBy("patient_id")
        .agg(
            count("unified_claim_id").alias("lifetime_visits"),
            sum("total_amount").alias("lifetime_spend"),
            min("service_date").alias("first_visit_date"),
            max("service_date").alias("last_visit_date")
        )
        .withColumn("_gold_processed_at", current_timestamp())
    )
    
    df_patient.write.format("delta").mode("overwrite").save(target_path)
    print(f"Successfully refreshed: {target_path}")

def build_gold_layer(spark: SparkSession, base_adls: str):
    source_path = f"{base_adls}/delta/silver_unified_claims"
    
    print(f"Reading Silver layer data from: {source_path}")
    # Load the unified Silver table once to use for multiple Gold tables
    silver_df = spark.read.format("delta").load(source_path)
    
    # Generate business reporting tables
    build_financial_summary(silver_df, base_adls)
    build_patient_summary(silver_df, base_adls)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_account", default="lshc", help="Azure Storage Account name")
    parser.add_argument("--container", default="landing-zone", help="ADLS Container name")
    args, unknown = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    base_adls = f"abfss://{args.container}@{args.storage_account}.dfs.core.windows.net"

    build_gold_layer(spark, base_adls)