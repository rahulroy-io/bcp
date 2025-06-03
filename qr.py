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

fact_job_run
├─ run_id (string/uuid)
│   └─ Unique identifier for this job execution (generated at job start).
│
├─ job_name (string)
│   └─ Full canonical job name (e.g., minda_marketingdbo_tablesets_mssql_jdbc_s3_ingest).
│
├─ env (string)
│   └─ Environment where it ran (dev / uat / prod).
│
├─ client (string)
│   └─ Business client name parsed from job_name (e.g., minda).
│
├─ domain (string)
│   └─ Logical domain or business area (e.g., marketingdbo).
│
├─ entity (string)
│   └─ Entity being processed (e.g., tablesets).
│
├─ source (string)
│   └─ Source system or technology (e.g., mssql).
│
├─ target (string)
│   └─ Target system (e.g., s3).
│
├─ action (string)
│   └─ Action type (e.g., ingest).
│
├─ start_ts (timestamp)
│   └─ Timestamp when job started.
│
├─ end_ts (timestamp)
│   └─ Timestamp when job finished.
│
├─ duration_sec (long)
│   └─ Total runtime in seconds (end_ts − start_ts).
│
├─ status (string)
│   └─ Final outcome: “running” (initial), then “success” / “fail” / “partial.”
│
├─ overall_error_msg (string)
│   └─ If something blew up, the highest-level error message; NULL if successful.
│
├─ config_checksum (string)
│   └─ Hash or fingerprint of job arguments or configuration files (for reproducibility).
│
└─ custom_md (string/json)
    └─ Free-form JSON blob for any additional tags or future metadata (e.g., Git commit, run parameters).

fact_job_event
├─ event_id (string/uuid)
│   └─ Unique identifier for this event row.
│
├─ run_id (string)
│   └─ Foreign key back to fact_job_run.run_id.
│
├─ step_name (string)
│   └─ Logical step (e.g., READ_JDBC, TRANSFORM, WRITE_S3, COMPACTION).
│
├─ event_type (string)
│   └─ One of: START / END / ERROR / INFO / METADATA.
│       • START  → beginning of a step  
│       • END    → successful end of a step  
│       • ERROR  → something went wrong in this step  
│       • INFO   → informational note (e.g., validation passed)  
│       • METADATA → table schema or partition info, etc.
│
├─ step_order (int)
│   └─ Optional numeric ordering of steps (1, 2, 3…).
│
├─ event_ts (timestamp)
│   └─ When this event was emitted.
│
├─ status (string)
│   └─ “success” / “fail” / “warn” (for INFO or ERROR events).
│
├─ message (string)
│   └─ Human-readable description or error text.
│
└─ event_payload (string/json)
    └─ Any extra details as JSON:  
        • For METADATA: { "table_path": "...", "columns": [...], "partition_cols": [...], "file_format": "parquet", ... }  
        • For ERROR: full stack trace, exception type, etc.  
        • For INFO: schema-validation results, etc.

fact_job_metric
├─ metric_id (string/uuid)
│   └─ Unique identifier for this metric row.
│
├─ run_id (string)
│   └─ Foreign key back to fact_job_run.run_id.
│
├─ step_name (string)
│   └─ Step that generated this metric (e.g., READ_JDBC, VALIDATE, WRITE_S3).
│
├─ metric_name (string)
│   └─ E.g., source_row_cnt, target_row_cnt, dq_rule_passed.
│
├─ metric_value (double)
│   └─ Numeric value: row count, file size in MB, or 1/0 for boolean pass/fail.
│
├─ metric_unit (string)
│   └─ Unit of measure: “rows” / “MB” / “pass” (for boolean) / seconds, etc.
│
├─ created_ts (timestamp)
│   └─ When this metric was recorded.
│
└─ metric_payload (string/json)
    └─ If the metric is complex, you can pack extra details here (e.g., histogram bins,
       DQ rule details) as JSON.

— 

**Notes on where each category fits:**
- **Run-level info, start/end, overall status →** `fact_job_run`.
- **Per-step START/END, table schema events, exceptions, and general “INFO” events →** `fact_job_event`.
- **Purely numeric results (DQ rules, row counts, file sizes) →** `fact_job_metric`.

You can build any necessary views later; this tree shows exactly which column lives in which table and why.
