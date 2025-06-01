import logging
from abc import ABC, abstractmethod
import argparse
import json
from pyspark.sql import SparkSession

class BaseIngestionJob(ABC):
    """
    Abstract base class for data ingestion jobs.
    SparkSession and AWS clients are provided externally.
    """
    def __init__(self, config: dict, spark: SparkSession, s3_client=None, **kwargs):
        """Initialize job with config and externally provided clients."""
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=logging.INFO)
        self.spark = spark
        self.s3_client = s3_client
        self.setup()

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

# -----------------------------------------------------------------------------
# Example Subclass (JDBC → S3 using provided Spark and S3 client)
# -----------------------------------------------------------------------------
class JdbcToS3IngestionJob(BaseIngestionJob):
    def _init_source_connection(self):
        src = self.config["source"]
        self.jdbc_url = src["url"]
        self.jdbc_table = src["table"]
        self.jdbc_props = {"user": src.get("user"), "password": src.get("password"), "driver": src.get("driver", "org.postgresql.Driver")}
        self.logger.info("JDBC source configured: %s", self.jdbc_url)

    def _init_target_connection(self):
        tgt = self.config["target"]
        self.bucket = tgt["bucket"]
        self.path = tgt["path"].rstrip("/")
        self.logger.info("S3 target configured: s3://%s/%s", self.bucket, self.path)

    def read_data(self):
        self.logger.info("Reading data from JDBC: %s.%s", self.jdbc_url, self.jdbc_table)
        df = self.spark.read.format("jdbc").options(
            url=self.jdbc_url,
            dbtable=self.jdbc_table,
            **self.jdbc_props
        ).load()
        return df

    def transform_data(self, df):
        self.logger.info("Transforming data: filtering on ingest_date")
        ingest_date = self.config.get("ingest_date")
        df_filtered = df.filter(df["event_date"] == ingest_date)
        from pyspark.sql.functions import lit
        df_final = df_filtered.withColumn("_ingest_date", lit(ingest_date))
        return df_final

    def write_data(self, df):
        target_path = f"s3://{self.bucket}/{self.path}/ingest_date={self.config.get('ingest_date')}"
        self.logger.info("Writing data to S3 Path: %s", target_path)
        df.write.mode("append").parquet(target_path)
        marker_key = f"{self.path}/ingest_date={self.config.get('ingest_date')}/_SUCCESS"
        if self.s3_client:
            self.s3_client.put_object(Bucket=self.bucket, Key=marker_key, Body=b"")
            self.logger.info("Wrote _SUCCESS marker to s3://%s/%s", self.bucket, marker_key)
        else:
            self.logger.warning("No S3 client provided; skipping _SUCCESS marker")

# -----------------------------------------------------------------------------
# Usage Example (not as CLI)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import boto3
    from pyspark.sql import SparkSession

    # Load config
    parser = argparse.ArgumentParser(description="JDBC to S3 Ingestion with Provided Clients")
    parser.add_argument("--config", required=True, help="Path to JSON config file for the job")
    args = parser.parse_args()

    with open(args.config, 'r') as f:
        config = json.load(f)

    # Assume SparkSession and boto3 client are created externally
    spark = SparkSession.builder.appName(config.get("job_name", "IngestionJob")).getOrCreate()
    s3_client = boto3.client("s3", region_name=config.get("aws", {}).get("region_name", None))

    job = JdbcToS3IngestionJob(config, spark=spark, s3_client=s3_client)
    job.run()
