from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import uuid

def write_job_metadata_to_hudi(
    spark: SparkSession,
    hudi_table_path: str,
    table_name: str,
    records: list,
    hudi_database: str = "default",
    partition_keys: list = ["job_id", "dt"],
    record_key: str = "record_id"
):
    """
    Insert job metadata records into Hudi table (MOR, insert-only).
    """
    # Convert records list of dicts to DataFrame
    df = spark.createDataFrame(records)

    # Hudi options for insert-only, MOR
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.partitionpath.field": ",".join(partition_keys),
        "hoodie.datasource.write.precombine.field": "timestamp",  # Not used but required
        "hoodie.upsert.shuffle.parallelism": 1,
        "hoodie.insert.shuffle.parallelism": 1,
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": hudi_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": ",".join(partition_keys),
        "hoodie.datasource.hive_sync.use_jdbc": "false",  # For Glue/Athena
        "hoodie.cleaner.policy.failed.writes": "EAGER"
    }

    # Write DataFrame to Hudi table
    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(hudi_table_path)
    )


records = [
    {
        "record_id": str(uuid.uuid4()),
        "job_id": "job_001",
        "dt": datetime.now().strftime("%Y-%m-%d"),
        "status": "SUCCESS",
        "timestamp": datetime.now().isoformat(),
        "message": "Job executed successfully",
        # ... other fields
    },
    # ...more records
]
write_job_metadata_to_hudi(
    spark=spark,
    hudi_table_path="s3://bucket/path/to/hudi_table",
    table_name="job_metadata",
    records=records,
    hudi_database="job_logs"
)


from pyspark.sql import SparkSession

def run_hudi_compaction(
    spark: SparkSession,
    hudi_table_path: str,
    table_name: str
):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.compaction.async.enable": "false",  # Force sync compaction
        "hoodie.datasource.write.operation": "compaction",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": "job_logs",
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_fields": "job_id,dt"
    }
    # Create an empty DataFrame (compaction does not require new data)
    df = spark.createDataFrame([], schema="record_id string, job_id string, dt string, status string, timestamp string, message string")
    (
        df.write.format("hudi")
        .options(**hudi_options)
        .mode("append")
        .save(hudi_table_path)
    )
