# dynamodb_glue_logger.py (Enhanced for Structured Payloads)
"""
Plug-and-play DynamoDB metadata logger for AWS Glue (Spark, Python Shell, or general Python).
- Drop into S3, zip, and import as an extra file for Glue jobs.
- Handles table creation, inserts, basic querying, error handling, and schema evolution.
- Supports job- or event-level logging with optional sort key for multi-event logs.
- Promotes selected metadata fields to top-level attributes for efficient querying.
"""
import boto3
import datetime
import time
from botocore.exceptions import ClientError

class GlueDynamoLogger:
    # Choose which fields to promote to top-level for easy query/filter
    PROMOTE_FIELDS = [
        'event_type', 'event_name', 'event_state', 'job_run_id', 'event_ts_epoch', 'event_ts_isoformat', 'job_type', 'env', 'step', 'record_id', 'event_date', 'task', 'metric_name', 'metric_value'
    ]
    def __init__(self, table_name, region_name="ap-south-1", partition_key="job_name", sort_key="event_ts_isoformat"):
        self.dynamodb = boto3.resource("dynamodb", region_name=region_name)
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.table = self._ensure_table_exists()

    def _ensure_table_exists(self):
        try:
            table = self.dynamodb.Table(self.table_name)
            table.load()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                key_schema = [{"AttributeName": self.partition_key, "KeyType": "HASH"}]
                attr_defs = [{"AttributeName": self.partition_key, "AttributeType": "S"}]
                if self.sort_key:
                    key_schema.append({"AttributeName": self.sort_key, "KeyType": "RANGE"})
                    attr_defs.append({"AttributeName": self.sort_key, "AttributeType": "S"})
                table = self.dynamodb.create_table(
                    TableName=self.table_name,
                    KeySchema=key_schema,
                    AttributeDefinitions=attr_defs,
                    BillingMode="PAY_PER_REQUEST",
                )
                table.wait_until_exists()
            else:
                raise
        return self.dynamodb.Table(self.table_name)

    def log_event(self, job_id, status, metadata=None, event_id=None, retries=3, backoff=1.0):
        # Promote selected fields from metadata to top-level for queryability
        item = {
            self.partition_key: str(job_id),
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "status": status,
        }
        if self.sort_key:
            item[self.sort_key] = event_id or item["timestamp"]
        if metadata:
            for field in self.PROMOTE_FIELDS:
                if field in metadata:
                    item[field] = metadata[field]
            item["payload"] = metadata # store full original payload as 'payload'
        for attempt in range(retries):
            try:
                self.table.put_item(Item=item)
                return
            except ClientError as e:
                if attempt < retries - 1 and e.response["Error"]["Code"] in [
                    "ProvisionedThroughputExceededException", "ThrottlingException"]:
                    time.sleep(backoff * (2 ** attempt))
                else:
                    raise

    def get_log(self, job_id, event_id=None):
        key = {self.partition_key: str(job_id)}
        if self.sort_key and event_id:
            key[self.sort_key] = event_id
        try:
            resp = self.table.get_item(Key=key)
            return resp.get("Item")
        except ClientError as e:
            print(f"Error getting item: {e}")
            return None

    def query_by_field(self, field, value, limit=100):
        # Scan-based filtering: efficient if field is a promoted field
        try:
            resp = self.table.scan(
                FilterExpression=f"#{field} = :val",
                ExpressionAttributeNames={f"#{field}": field},
                ExpressionAttributeValues={":val": value},
                Limit=limit,
            )
            return resp.get("Items", [])
        except ClientError as e:
            print(f"Error querying by field: {e}")
            return []

    def query_by_job(self, job_id):
        if not self.sort_key:
            item = self.get_log(job_id)
            return [item] if item else []
        try:
            resp = self.table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key(self.partition_key).eq(str(job_id))
            )
            return resp.get("Items", [])
        except ClientError as e:
            print(f"Error querying by job: {e}")
            return []

# -------------------
# Example usage (Glue ETL or Python Shell, plug & play):
#
# from dynamodb_glue_logger import GlueDynamoLogger
#
# logger = GlueDynamoLogger(
#     table_name="your_dynamodb_metadata_table",
#     region_name="ap-south-1",
#     partition_key="job_name",        # Or "job_run_id"
#     sort_key="event_ts_isoformat",   # Or "record_id"
# )
#
# payload = {
#     "task": "TASK03",
#     "event_type": "JOB_STATUS",
#     "event_name": "INGESTION_STARTED",
#     "event_state": "SUCCESS",
#     "event_payload": "",
#     "metric_name": "",
#     "metric_value": "",
#     "job_run_id": "20240707123456",
#     "event_ts_epoch": 1720457692.63,
#     "event_ts_isoformat": "2024-07-08T08:34:52.634262",
#     "job_type": "nbjob",
#     "env": "uat",
#     "step": "1",
#     "record_id": "some-uuid",
#     "event_date": "2024-07-08"
# }
#
# logger.log_event(
#     job_id=payload["job_name"],
#     status=payload["event_state"],   # e.g. "SUCCESS"
#     metadata=payload,
#     event_id=payload["event_ts_isoformat"],  # as sort key
# )
#
# # Fetch all events for a job:
# items = logger.query_by_job(payload["job_name"])
# for i in items:
#     print(i)
#
# # Query by any promoted field, e.g. all failures:
# failed = logger.query_by_field("event_state", "FAILURE")
# print("Failed events:", failed)
