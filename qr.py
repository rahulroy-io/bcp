import logging
from abc import ABC, abstractmethod
import argparse
import json
import boto3
from pyspark.sql import SparkSession

class BaseIngestionJob(ABC):
    """
    Abstract base class for data ingestion jobs.
    Defines setup, read, transform, write, and run steps.
    Practical setup includes SparkSession for ETL and boto3 for S3.
    """
    def __init__(self, config: dict):
        """Initialize job with config."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.INFO)
        self._init_spark()
        self._init_aws_clients()
        self.setup()

    def _init_spark(self):
        """Create SparkSession with common settings."""
        spark_conf = self.config.get("spark", {})
        builder = SparkSession.builder.appName(self.config.get("job_name", "IngestionJob"))
        for key, value in spark_conf.items():
            builder = builder.config(key, value)
        self.spark = builder.getOrCreate()
        self.logger.info("SparkSession initialized")

    def _init_aws_clients(self):
        """Initialize AWS clients (e.g., S3, SNS) if needed."""
        aws_conf = self.config.get("aws", {})
        boto3_kwargs = {}
        if aws_conf.get("region_name"):  boto3_kwargs["region_name"] = aws_conf["region_name"]
        if aws_conf.get("profile_name"):  boto3.setup_default_session(profile_name=aws_conf["profile_name"])
        self.s3_client = boto3.client("s3", **boto3_kwargs)
        self.logger.info("AWS S3 client initialized")
        # Additional services can be initialized similarly

    def setup(self):
        """Setup source and target connections."""
        self.logger.info("Running setup steps")
        self._init_source_connection()
        self._init_target_connection()

    @abstractmethod
    def _init_source_connection(self):
        """Initialize connection to the data source (e.g., JDBC)."""
        pass

    @abstractmethod
    def _init_target_connection(self):
        """Initialize connection to the data target (e.g., S3 path, database)."""
        pass

    @abstractmethod
    def read_data(self):
        """Read raw data from source, return as Spark DataFrame."""
        pass

    @abstractmethod
    def transform_data(self, df):
        """Apply transformations on DataFrame; return transformed DataFrame."""
        pass

    @abstractmethod
    def write_data(self, df):
        """Write DataFrame to target (e.g., Parquet on S3, JDBC)."""
        pass

    def run(self):
        """Orchestrate the ingestion: read → transform → write."""
        self.logger.info("Starting ingestion run")
        raw_df = self.read_data()
        transformed_df = self.transform_data(raw_df)
        self.write_data(transformed_df)
        self.logger.info("Ingestion run completed")
        self.spark.stop()

# -----------------------------------------------------------------------------
# Example Practical Subclass
# -----------------------------------------------------------------------------
class JdbcToS3IngestionJob(BaseIngestionJob):
    def _init_source_connection(self):
        """Set up JDBC options for Spark read."""
        src = self.config["source"]
        self.jdbc_url = src["url"]
        self.jdbc_table = src["table"]
        self.jdbc_props = {"user": src.get("user"), "password": src.get("password"), "driver": src.get("driver", "org.postgresql.Driver")}
        self.logger.info("JDBC source configured: %s", self.jdbc_url)

    def _init_target_connection(self):
        """Set up S3 paths for Parquet write."""
        tgt = self.config["target"]
        self.bucket = tgt["bucket"]
        self.path = tgt["path"].rstrip("/")
        self.logger.info("S3 target configured: s3://%s/%s", self.bucket, self.path)

    def read_data(self):
        """Read from JDBC source into a Spark DataFrame."""
        self.logger.info("Reading data from JDBC: %s.%s", self.jdbc_url, self.jdbc_table)
        df = self.spark.read.format("jdbc").options(
            url=self.jdbc_url,
            dbtable=self.jdbc_table,
            **self.jdbc_props
        ).load()
        self.logger.info("Read %d records", df.count())
        return df

    def transform_data(self, df):
        """Example transformation: filter and add ingest date."""
        self.logger.info("Transforming data: filtering new records")
        ingest_date = self.config.get("ingest_date")  # e.g., '2025-06-01'
        df_filtered = df.filter(df["event_date"] == ingest_date)
        df_with_meta = df_filtered.withColumn("_ingest_date", df_filtered["event_date"])
        self.logger.info("Transformed to %d records", df_with_meta.count())
        return df_with_meta

    def write_data(self, df):
        """Write DataFrame to S3 as partitioned Parquet."""
        target_path = f"s3://{self.bucket}/{self.path}/ingest_date={self.config.get('ingest_date')}"
        self.logger.info("Writing data to S3 Path: %s", target_path)
        df.write.mode("append").parquet(target_path)
        # Optionally, write a _SUCCESS marker
        marker_key = f"{self.path}/ingest_date={self.config.get('ingest_date')}/_SUCCESS"
        self.s3_client.put_object(Bucket=self.bucket, Key=marker_key, Body=b"")
        self.logger.info("Wrote _SUCCESS marker to s3://%s/%s", self.bucket, marker_key)

# -----------------------------------------------------------------------------
# Hudi Example Subclass for COW
# -----------------------------------------------------------------------------
class JdbcToHudiIngestionJob(BaseIngestionJob):
    def _init_source_connection(self):
        src = self.config["source"]
        self.jdbc_url = src["url"]
        self.jdbc_table = src["table"]
        self.jdbc_props = {"user": src.get("user"), "password": src.get("password"), "driver": src.get("driver", "org.postgresql.Driver")}
        self.logger.info("JDBC source configured for Hudi: %s", self.jdbc_url)

    def _init_target_connection(self):
        tgt = self.config["target"]
        self.hudi_table_name = tgt["table_name"]
        self.base_path = f"s3://{tgt['bucket']}/{tgt['path']}"
        self.logger.info("Hudi target configured: %s at %s", self.hudi_table_name, self.base_path)

    def read_data(self):
        self.logger.info("Reading data from JDBC for Hudi: %s.%s", self.jdbc_url, self.jdbc_table)
        return self.spark.read.format("jdbc").options(
            url=self.jdbc_url,
            dbtable=self.jdbc_table,
            **self.jdbc_props
        ).load()

    def transform_data(self, df):
        self.logger.info("Transforming data for Hudi: dedup and add metadata columns")
        # Example: drop duplicates, add partition column
        ingest_date = self.config.get("ingest_date")
        df_clean = df.dropDuplicates([self.config.get("record_key")])
        from pyspark.sql.functions import lit
        df_final = df_clean.withColumn("partition_date", lit(ingest_date))
        return df_final

    def write_data(self, df):
        hudi_opts = {
            'hoodie.table.name': self.hudi_table_name,
            'hoodie.datasource.write.recordkey.field': self.config.get('record_key'),
            'hoodie.datasource.write.precombine.field': self.config.get('precombine_field'),
            'hoodie.datasource.write.partitionpath.field': 'partition_date',
            'hoodie.datasource.write.table.name': self.hudi_table_name,
            'hoodie.datasource.write.operation': 'upsert',
            'hoodie.upsert.shuffle.parallelism': 2,
            'hoodie.insert.shuffle.parallelism': 2,
            'hoodie.datasource.write.hive_style_partitioning': 'true'
        }
        self.logger.info("Writing Hudi table: %s", self.hudi_table_name)
        df.write.format("org.apache.hudi").options(**hudi_opts).mode("append").save(self.base_path)
        self.logger.info("Hudi ingestion complete. Base path: %s", self.base_path)

# -----------------------------------------------------------------------------
# CLI Entrypoint
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Practical Ingestion Job Template")
    parser.add_argument("--config", required=True, help="Path to JSON config file for the job")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    job_type = config.get("job_type", "jdbc_to_s3")
    if job_type == "jdbc_to_s3":
        job = JdbcToS3IngestionJob(config)
    elif job_type == "jdbc_to_hudi":
        job = JdbcToHudiIngestionJob(config)
    else:
        raise ValueError(f"Unsupported job_type: {job_type}")

    job.run()

if __name__ == "__main__":
    main()

# -----------------------------------------------------------------------------
# Example JSON config (config.json):
# {
#   "job_name": "sales_jdbc_to_s3",
#   "job_type": "jdbc_to_s3",               # or jdbc_to_hudi
#   "spark": {                                 # optional Spark configs
#     "spark.executor.memory": "4g"
#   },
#   "aws": {                                  # optional AWS settings
#     "region_name": "us-east-1",            # override boto3 region
#     "profile_name": "default"
#   },
#   "ingest_date": "2025-06-01",
#   "source": {
#       "type": "jdbc",
#       "url": "jdbc:postgresql://hostname:5432/dbname",
#       "table": "public.sales",
#       "user": "username",
#       "password": "password",
#       "driver": "org.postgresql.Driver"
#   },
#   "target": {
#       "type": "s3",
#       "bucket": "my-data-bucket",
#       "path": "ingestion/raw/sales"
#       # For Hudi:
#       # "table_name": "sales_hudi",
#       # "bucket": "my-data-bucket",
#       # "path": "ingestion/hudi/sales"
#   },
#   "record_key": "order_id",               # for Hudi
#   "precombine_field": "last_updated"       # for Hudi
# }

