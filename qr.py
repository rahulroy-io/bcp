import logging
import argparse
import json
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException

class DataIngestionJob:
    """
    A simple data ingestion job template with enhanced validations and production best practices.
    Expects SparkSession and optional S3 client to be passed externally.
    Includes config sanity checks, validation, exception handling, and metrics logging.
    """
    def __init__(self, config: dict, spark: SparkSession, s3_client=None, sns_client=None):
        """Initialize job with config, SparkSession, and AWS clients."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.INFO)
        self.spark = spark
        self.s3_client = s3_client
        self.sns_client = sns_client
        self._validate_config()
        self._init_connections()

    def _validate_config(self):
        """Ensure required config keys and subkeys exist."""
        self.logger.info("Validating configuration keys")
        required_keys = ["source", "target", "ingest_date"]
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required config key: {key}")
        # Validate source subkeys
        src = self.config["source"]
        for sub in ["url", "table", "user", "password"]:
            if sub not in src or not src.get(sub):
                raise ValueError(f"Missing or empty source parameter: {sub}")
        # Validate target subkeys
        tgt = self.config["target"]
        for sub in ["bucket", "path"]:
            if sub not in tgt or not tgt.get(sub):
                raise ValueError(f"Missing or empty target parameter: {sub}")
        self.logger.info("Configuration validation passed")

    def _init_connections(self):
        """Initialize any source/target connections, based on config."""
        src = self.config["source"]
        self.jdbc_url = src.get("url")
        self.jdbc_table = src.get("table")
        self.jdbc_props = {
            "user": src.get("user"),
            "password": src.get("password"),
            "driver": src.get("driver", "org.postgresql.Driver")
        }
        self.logger.info(f"Configured JDBC source: {self.jdbc_url}, table: {self.jdbc_table}")

        tgt = self.config["target"]
        self.bucket = tgt.get("bucket")
        self.path = tgt.get("path", "").rstrip("/")
        self.logger.info(f"Configured S3 target: s3://{self.bucket}/{self.path}")

        # Optionally, set up SNS for notifications
        self.sns_topic_arn = self.config.get("sns_topic_arn")

    def read_data(self) -> DataFrame:
        """Read data from JDBC source using Spark; return a DataFrame with retry logic."""
        self.logger.info(f"Reading data from JDBC: {self.jdbc_url}.{self.jdbc_table}")
        attempt = 0
        max_retries = self.config.get("retries", 3)
        while attempt < max_retries:
            try:
                start_time = time.time()
                df = self.spark.read.format("jdbc").options(
                    url=self.jdbc_url,
                    dbtable=self.jdbc_table,
                    **self.jdbc_props
                ).load()
                count = df.count()
                self.logger.info(f"Read {count} records from source in {time.time() - start_time:.2f} seconds.")
                return df
            except AnalysisException as e:
                attempt += 1
                self.logger.error(f"JDBC read failed on attempt {attempt}: {e}")
                time.sleep(5)
        raise RuntimeError("Max retries reached: failed to read data from JDBC source.")

    def transform_data(self, df: DataFrame) -> DataFrame:
        """Apply transformations to DataFrame; return transformed DataFrame with schema checks."""
        self.logger.info("Transforming data")
        ingest_date = self.config.get("ingest_date")
        # Example: filter rows by ingest_date column
        try:
            df_filtered = df.filter(df["event_date"] == ingest_date)
        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise
        from pyspark.sql.functions import lit
        df_with_metadata = df_filtered.withColumn("_ingest_date", lit(ingest_date))
        self.logger.info(f"Transformed data to {df_with_metadata.count()} records.")

        # Schema validation if provided
        schema_config = self.config.get("schema", {})
        if schema_config:
            self._validate_schema(df_with_metadata, schema_config)
        return df_with_metadata

    def _validate_schema(self, df: DataFrame, schema_config: dict):
        """Ensure DataFrame columns match expected schema types."""
        self.logger.info("Validating schema against config")
        actual_types = dict(df.dtypes)
        for col, expected_type in schema_config.items():
            actual_type = actual_types.get(col)
            if actual_type is None:
                raise ValueError(f"Schema validation failed: missing column {col}")
            if actual_type.lower() != expected_type.lower():
                raise ValueError(f"Schema validation failed: column {col} is type {actual_type}, expected {expected_type}")
        self.logger.info("Schema validation passed")

    def validate_data(self, df: DataFrame) -> bool:
        """Validate DataFrame before writing. Returns True if valid, else False."""
        self.logger.info("Validating data before write")
        # 1. Non-zero record count
        count = df.count()
        if count == 0:
            self.logger.error("Validation failed: DataFrame has zero records.")
            return False

        # 2. Check required columns exist
        required_columns = self.config.get("required_columns", [])
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            self.logger.error(f"Validation failed: Missing columns {missing_cols}")
            return False

        # 3. Null checks: ensure critical fields have low null ratio
        null_checks = self.config.get("null_checks", {})
        for col, max_null_ratio in null_checks.items():
            null_fraction = df.filter(df[col].isNull()).count() / float(count)
            if null_fraction > max_null_ratio:
                self.logger.error(f"Validation failed: Column {col} null ratio {null_fraction:.2f} exceeds {max_null_ratio}")
                return False

        # 4. Domain rules: e.g., amounts must be >= 0
        domain_rules = self.config.get("domain_rules", {})
        for col, rule in domain_rules.items():
            if rule == "non_negative":
                invalid = df.filter(df[col] < 0).count()
                if invalid > 0:
                    self.logger.error(f"Validation failed: Column {col} has {invalid} negative values.")
                    return False

        self.logger.info("Validation passed")
        return True

    def write_data(self, df: DataFrame):
        """Write DataFrame to S3 as partitioned Parquet with exception handling, idempotency, and _SUCCESS marker."""
        if not self.validate_data(df):
            raise ValueError("Data validation failed; aborting write.")

        ingest_date = self.config.get("ingest_date")
        partition_key = self.config.get("partition_key", "ingest_date")
        overwrite = self.config.get("overwrite_partition", False)
        target_base = f"s3://{self.bucket}/{self.path}"
        target_path = f"{target_base}/{partition_key}={ingest_date}"

        write_mode = "overwrite" if overwrite else "append"
        self.logger.info(f"Writing data to S3 path: {target_path} with mode {write_mode}")

        try:
            start_time = time.time()
            df.write.mode(write_mode).parquet(target_path)
            self.logger.info(f"Write completed in {time.time() - start_time:.2f} seconds")
        except Exception as e:
            self.logger.error(f"Parquet write failed: {e}", exc_info=True)
            raise

        # Write _SUCCESS marker
        if self.s3_client:
            marker_key = f"{self.path}/{partition_key}={ingest_date}/_SUCCESS"
            try:
                self.s3_client.put_object(Bucket=self.bucket, Key=marker_key, Body=b"")
                self.logger.info(f"_SUCCESS marker written to s3://{self.bucket}/{marker_key}")
            except Exception as e:
                self.logger.warning(f"Failed to write _SUCCESS marker: {e}", exc_info=True)
        else:
            self.logger.warning("No S3 client provided; skipping _SUCCESS marker.")

        # Send success notification
        if self.sns_client and self.sns_topic_arn:
            try:
                message = f"Ingestion succeeded for date {ingest_date} at {target_path}"
                self.sns_client.publish(TopicArn=self.sns_topic_arn, Message=message)
                self.logger.info("Sent success notification to SNS")
            except Exception as e:
                self.logger.warning(f"Failed to send SNS notification: {e}", exc_info=True)

    def run(self):
        """Execute the ingestion: read → transform → validate → write with metrics."""
        job_start = time.time()
        self.logger.info("Starting ingestion job")
        try:
            df_raw = self.read_data()
            df_transformed = self.transform_data(df_raw)
            self.write_data(df_transformed)
            duration = time.time() - job_start
            self.logger.info(f"Ingestion job completed in {duration:.2f} seconds.")
        except Exception as e:
            self.logger.error(f"Ingestion job failed: {e}", exc_info=True)
            # Optionally send failure notification
            if self.sns_client and self.sns_topic_arn:
                try:
                    self.sns_client.publish(TopicArn=self.sns_topic_arn, Message=f"Ingestion failed: {e}")
                except Exception:
                    pass
            raise

# -----------------------------------------------------------------------------
# Example usage without a CLI (Spark & AWS clients provided externally)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Data Ingestion Job")
    parser.add_argument("--config", required=True, help="Path to JSON config file for the job")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    # Optionally fetch secrets from AWS Secrets Manager
    secrets_name = config.get("secrets_manager_name")
    if secrets_name:
        import boto3
        secrets_client = boto3.client("secretsmanager", region_name=config.get("aws", {}).get("region_name"))
        secret_value = secrets_client.get_secret_value(SecretId=secrets_name)
        secret_data = json.loads(secret_value["SecretString"])
        # Override source credentials
        config["source"]["user"] = secret_data.get("user")
        config["source"]["password"] = secret_data.get("password")

    spark = SparkSession.builder.appName(config.get("job_name", "IngestionJob")).getOrCreate()
    import boto3
    s3_client = boto3.client("s3", region_name=config.get("aws", {}).get("region_name", None))
    sns_client = boto3.client("sns", region_name=config.get("aws", {}).get("region_name", None)) if config.get("sns_topic_arn") else None

    job = DataIngestionJob(config, spark=spark, s3_client=s3_client, sns_client=sns_client)
    job.run()

# -----------------------------------------------------------------------------
# Example JSON config (config.json):
# {
#   "job_name": "sales_ingestion",
#   "ingest_date": "2025-06-01",
#   "required_columns": ["order_id", "event_date", "amount"],
#   "schema": {"order_id": "string", "event_date": "string", "amount": "double"},
#   "null_checks": {"amount": 0.01},           # max 1% nulls allowed in amount
#   "domain_rules": {"amount": "non_negative"},
#   "partition_key": "event_date",            # override default partition column
#   "overwrite_partition": false,
#   "secrets_manager_name": "my/jdbc/creds",  # optional secret store name
#   "sns_topic_arn": "arn:aws:sns:us-east-1:123456789012:IngestionAlerts",
#   "retries": 3,
#   "source": {
#     "url": "jdbc:postgresql://hostname:5432/dbname",
#     "table": "public.sales",
#     "user": "",  # will be overridden by secrets
#     "password": "",
#     "driver": "org.postgresql.Driver"
#   },
#   "target": {
#     "bucket": "my-data-bucket",
#     "path": "ingestion/raw/sales"
#   },
#   "aws": {
#     "region_name": "us-east-1"
#   }
# }
