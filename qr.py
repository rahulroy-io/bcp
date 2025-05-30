import qrcode

# Create a QR code instance
qr = qrcode.QRCode(
    version=40,  # Specify the version (e.g., Version 40)
    error_correction=qrcode.constants.ERROR_CORRECT_H,  # Set error correction level
    box_size=10,  # Set the size of each QR code module (box)
    border=4  # Set the border size around the QR code
)

# Add data to the QR code
data = "Hello, QR Code!"
qr.add_data(data)

# Generate the QR code
qr.make(fit=True)

# Get the QR code image
qr_image = qr.make_image(fill_color="black", back_color="white")


CREATE EXTERNAL TABLE IF NOT EXISTS svoc_lookup (
  svoc_id DECIMAL(38, 10),
  mobile STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
LOCATION 's3://your-bucket-name/path/to/csv-data/'
TBLPROPERTIES (
  'skip.header.line.count' = '1'
);


# Save the QR code image
qr_image.save("qr_code.png")



def string_to_number(string):
    # Convert each character to its ASCII value and concatenate them
    number = ''.join(str(ord(char)) for char in string)
    return int(number)

def number_to_string(number):
    # Convert the number to a string and split it into pairs of digits
    number_str = str(number)
    digits = [number_str[i:i+2] for i in range(0, len(number_str), 2)]

    # Convert each pair of digits back to its corresponding character
    string = ''.join(chr(int(digit)) for digit in digits)
    return string

bcp "SELECT * FROM [testdatabase].[dbo].[employee]" QUERYOUT op -S sql-server-source.cfoapfkvmzlt.us-east-2.rds.amazonaws.com -U admin -P rahulroy53 -n

def uploadFile(inputStream, filePath, bucketName):
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    def isBucketExists():
        try:
            s3_resource.meta.client.head_bucket(Bucket=bucketName)
        except botocore.exceptions.ClientError as e:
            return False
        else :
            return True
    #logger  
    if (not isBucketExists()):
        raise Exception("Upload failed. Bucket {} does not exist".format(bucketName))

    obj = s3_resource.Object(bucketName, filePath)
    response = obj.put(Body=inputStream)
    res = response.get("ResponseMetadata")

    if res.get('HTTPStatusCode') == 200:
        job.logger().info(f, f"File uploaded at {filePath}")
        return True
    else :
        job.logger().info(f, f"Upload failed with HTTPStatusCode {res.get('HTTPStatusCode')}")
        return False

input_stream = bytes(json.dumps(data).encode('UTF-8'))
if uploadFile(input_stream, filepath, bucket_target) :
    job.logger().info(f, f"File s3a://{bucket_target}/{filepath} uploaded successfully")
    job.logger().info(f, f'###################_TASK-3_JOB_RUN_SUCCESSFULL_###################')
else :
    raise Exception(f"Upload Failed")


def send_request(params) :
    response = requests.get(url=base_api_url, params=params, auth=(user, pwd), headers=headers)
    if response.status_code != 200 :
        print(f"api response status code : {response.status_code}")
        raise Exception(f"api response status code : {response.status_code}")
    else :
        print(f"DATA COUNT {len(response.json()['result'])}")
        print(f"api headers {response.headers}")
        return response

with cf.ThreadPoolExecutor() as executor :
        results = executor.map(send_request, params_params)

for response in results :
    append response.json()['result']

input_stream = bytes(json.dumps(data).encode('UTF-8'))
if uploadFile(input_stream, filepath, bucket_target) :
    print(f"File s3a://{bucket_target}/{filepath} uploaded successfully")
    print(f'###################_TASK-3_JOB_RUN_SUCCESSFULL_###################')
else :
    raise Exception(f"Upload Failed")


def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
    complex_fields = dict([(field.name, field.dataType)
                            for field in df.schema.fields
                            if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    while len(complex_fields)!=0:
        col_name=list(complex_fields.keys())[0]
        # print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [col(col_name+'.'+k).alias(k) for k in [ n.name for n in  complex_fields[col_name]]]
            df=df.select("*", *expanded).drop(col_name)
    
        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == ArrayType):    
            df=df.withColumn(col_name,explode_outer(col_name))
    
        # recompute remaining Complex Fields in Schema       
        complex_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == ArrayType or  type(field.dataType) == StructType])
    return df

client = boto3.client('glue')

response = client.start_crawler(
                        Name=crawler
                    )
            
response_get = client.get_crawler(Name=crawler)
state = response_get["Crawler"]["State"]
job.logger().info(f, f"Crawler '{crawler}' is {state.lower()}.")
state_previous = state
while (state != "READY") :
    time.sleep(2)
    response_get = client.get_crawler(Name=crawler)
    state = response_get["Crawler"]["State"]
    if state != state_previous:
        job.logger().info(f, f"Crawler {crawler} is {state.lower()}.")
        state_previous = state


# Specify the name of the crawler you want to run
crawler_name = 'your-crawler-name'

try:
    # Start the crawler
    response = glue_client.start_crawler(Name=crawler_name)
    print("Crawler started successfully.")
except ClientError as e:
    if e.response['Error']['Code'] == 'CrawlerRunningException':
        print("Crawler is already running. Waiting for it to finish...")
        # You can wait for the crawler to finish here
        while True:
            try:
                response = glue_client.start_crawler(Name=crawler_name)
                break  # Crawler has finished, so it's safe to start it again
            except ClientError as e:
                if e.response['Error']['Code'] != 'CrawlerRunningException':
                    raise  # Something else went wrong, raise the exception
                time.sleep(30)  # Wait for a while and check again
    else:
        # Handle other ClientErrors here
        raise

REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$')

SELECT
  CASE
    WHEN email REGEXP '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 'true'
    ELSE 'false'
  END AS is_valid_email
FROM your_table;

SELECT *
FROM your_table
WHERE REGEXP_LIKE(email_column, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')

^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z]+$
^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$


SELECT email
FROM your_table
WHERE regexp_like(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,4}$');


import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

# Specify the bucket name
bucket_name = 'your-bucket-name'

# Initialize the continuation token to None to start from the beginning
continuation_token = None

while True:
    # List objects in the bucket with continuation token
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        ContinuationToken=continuation_token
    )

    # Process the objects in the current response
    for obj in response.get('Contents', []):
        print(f"Object Key: {obj['Key']}")

    # Check if there are more objects to fetch
    if response.get('IsTruncated', False):
        continuation_token = response['NextContinuationToken']
    else:
        break  # No more objects to fetch, exit the loop


Action = 'outbound' # [ingest, structured, curated]
Client = 'msil'
Source = 's3' #[api, s3, mssql]
Target = 'api' #[api, s3, mssql]
Domain = 'crm' #[servicenow, azuredevops]
EntityName = 'casemanagementcomplaint' #[feedback, complaint, contacts]

job_name = f"job_{Client}_{Domain}_{EntityName}_{Source}_to_{Target}_{Action}"
print (f"job_name : {job_name}")

project_name = f"{Client}_{Domain}_{EntityName}_{Source}_to_{Target}_{Action}"
print (f"project_name : {project_name}")

crawler_name = f"crawler_{Client}_{Domain}_{EntityName}_{Source}_to_{Target}_{Action}"
print (f"crawler_name : {crawler_name}")


import boto3
from botocore.exceptions import ClientError, PaginationError
from concurrent.futures import ThreadPoolExecutor, as_completed

class AppFlowManager:
    def __init__(self, region_name=None):
        """
        Initialize the AppFlowManager with a Boto3 client for AppFlow.

        :param region_name: AWS region name (e.g., 'us-east-1'). If None, uses the default region.
        """
        self.client = boto3.client('appflow', region_name=region_name)

    def start_flow(self, flow_name, client_token=None):
        """
        Start an AppFlow flow execution.

        :param flow_name: The name of the flow to start.
        :param client_token: (Optional) A unique, case-sensitive string to ensure idempotency.
        :return: The execution ID of the started flow.
        """
        try:
            response = self.client.start_flow(
                flowName=flow_name,
                clientToken=client_token  # Optional, can be omitted
            )
            execution_id = response.get('executionId')
            print(f"Started flow '{flow_name}'. Execution ID: {execution_id}")
            return execution_id
        except ClientError as e:
            print(f"Failed to start flow '{flow_name}': {e}")
            return None

    def get_execution_status(self, flow_name, execution_id):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        try:
            paginator = self.client.get_paginator('describe_flow_execution_records')
            page_iterator = paginator.paginate(flowName=flow_name)

            for page in page_iterator:
                for record in page.get('flowExecutions', []):
                    if record.get('executionId') == execution_id:
                        status = record.get('executionStatus')
                        result = record.get('executionResult')
                        print(f"Execution ID '{execution_id}' Status: {status}")
                        return status, result
            print(f"Execution ID '{execution_id}' not found for flow '{flow_name}'.")
            return None, None
        except (ClientError, PaginationError) as e:
            print(f"Error retrieving execution status for '{execution_id}': {e}")
            return None, None

    def create_or_update_flow(self, flow_name, flow_config):
        """
        Create a new flow or update an existing flow.

        :param flow_name: The name of the flow to create or update.
        :param flow_config: A dictionary containing the flow configuration.
        :return: True if the flow was created or updated successfully, False otherwise.
        """
        try:
            if self.flow_exists(flow_name):
                print(f"Flow '{flow_name}' exists. Updating...")
                self.update_flow(flow_name, flow_config)
                print(f"Flow '{flow_name}' updated successfully.")
            else:
                print(f"Flow '{flow_name}' does not exist. Creating...")
                self.create_flow(flow_name, flow_config)
                print(f"Flow '{flow_name}' created successfully.")
            return True
        except ClientError as e:
            print(f"Failed to create or update flow '{flow_name}': {e}")
            return False

    def flow_exists(self, flow_name):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        try:
            self.client.describe_flow(flowName=flow_name)
            return True
        except self.client.exceptions.ResourceNotFoundException:
            return False
        except ClientError as e:
            print(f"Error checking if flow '{flow_name}' exists: {e}")
            raise

    def create_flow(self, flow_name, flow_config):
        """
        Create a new flow with the given configuration.

        :param flow_name: The name of the flow to create.
        :param flow_config: A dictionary containing the flow configuration.
        """
        try:
            self.client.create_flow(
                flowName=flow_name,
                triggerConfig=flow_config['triggerConfig'],
                sourceFlowConfig=flow_config['sourceFlowConfig'],
                destinationFlowConfigList=flow_config['destinationFlowConfigList'],
                tasks=flow_config['tasks'],
                description=flow_config.get('description', ''),
                tags=flow_config.get('tags', {})
            )
        except ClientError as e:
            print(f"Failed to create flow '{flow_name}': {e}")
            raise

    def update_flow(self, flow_name, flow_config):
        """
        Update an existing flow with the given configuration.

        :param flow_name: The name of the flow to update.
        :param flow_config: A dictionary containing the updated flow configuration.
        """
        try:
            self.client.update_flow(
                flowName=flow_name,
                triggerConfig=flow_config['triggerConfig'],
                sourceFlowConfig=flow_config['sourceFlowConfig'],
                destinationFlowConfigList=flow_config['destinationFlowConfigList'],
                tasks=flow_config['tasks'],
                description=flow_config.get('description', '')
            )
        except ClientError as e:
            print(f"Failed to update flow '{flow_name}': {e}")
            raise

    def execute_async_flows(self, flow_name, date_ranges, max_concurrent=10):
        """
        Execute multiple flows asynchronously with a ThreadPoolExecutor.

        :param flow_name: The name of the flow to start.
        :param date_ranges: List of date range tuples (start_date, end_date).
        :param max_concurrent: Maximum number of concurrent executions.
        """
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            future_to_date_range = {
                executor.submit(self.start_flow, flow_name, client_token=f'{start_date}_{end_date}'): (start_date, end_date)
                for start_date, end_date in date_ranges
            }

            for future in as_completed(future_to_date_range):
                start_date, end_date = future_to_date_range[future]
                try:
                    execution_id = future.result()
                    if execution_id:
                        status, result = self.get_execution_status(flow_name, execution_id)
                        print(f"Flow execution for date range ({start_date} to {end_date}) status: {status}")
                        if result:
                            print(f"Execution result for date range ({start_date} to {end_date}): {result}")
                except Exception as e:
                    print(f"Error executing flow for date range ({start_date} to {end_date}): {e}")

# Example usage:

if __name__ == "__main__":
    # Initialize the AppFlowManager
    appflow_manager = AppFlowManager(region_name='us-east-1')

    # Define the flow name
    flow_name = 'my_app_flow'

    # Define the flow configuration
    flow_config = {
        'triggerConfig': {
            'triggerType': 'OnDemand'  # Options: 'OnDemand', 'Scheduled', 'Event'
        },
        'sourceFlowConfig': {
            'connectorType': 'SAPOData',  # Your source connector type
            'connectorProfileName': 'my_sap_connection',  # Name of your connector profile
            'sourceConnectorProperties': {
                'SAPOData': {
                    'objectPath': 'path/to/your/object'  # Replace with your object path
                }
            }
        },
        'destinationFlowConfigList': [
            {
                'connectorType': 'S3',  # Your destination connector type
                'connectorProfileName': '',  # Not required for Amazon S3
                'destinationConnectorProperties': {
                    'S3': {
                        'bucketName': 'my-destination-bucket',
                        'bucketPrefix': 'my/prefix/',
                        's3OutputFormatConfig': {
                            'fileType': 'PARQUET'  # Options: 'CSV', 'JSON', 'PARQUET'
                        }
                    }
                }
            }
        ],
        'tasks': [
            {
                'sourceFields': ['*'],  # Fields to include, '*' means all fields
                'connectorOperator': {
                    'SAPOData': 'NO_OP'  # Operator, e.g., 'NO_OP', 'BETWEEN', etc.
                },
                'taskType': 'Filter',  # Options: 'Filter', 'Map', etc.
                'taskProperties': {
                    'DATA_TYPE': 'datetime',
                    'LOWER_BOUND': '2023-01-01T00:00:00Z',
                    'UPPER_BOUND': '2023-01-07T23:59:59Z'
                }
            }
        ],
        'description': 'Flow to ingest data from SAP OData to S3 in Parquet format',
        'tags': {
            'Project': 'DataIngestion'
        }
    }

    # Create or update the flow
    appflow_manager.create_or_update_flow(flow_name, flow_config)

    # Define date ranges for historical data
    from datetime import datetime, timedelta
    date_ranges = [
        (start_date.strftime('%Y-%m-%d'), (start_date + timedelta(days=6)).strftime('%Y-%m-%d'))
        for start_date in (datetime(2023, 1, 1) + timedelta(weeks=n) for n in range(52))
    ]

    # Execute the flows asynchronously
    appflow_manager.execute_async_flows(flow_name, date_ranges, max_concurrent=10)


{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"iam:GetRole",
				"iam:PassRole"
			],
			"Resource": "arn:aws:iam::444215702433:role/glueRole"
		}
	]
}

#######################################TASK-0#################################################
# IMPORTS

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql import types as T

import boto3

import json
import requests
from datetime import datetime as dt

import time
from botocore.exceptions import ClientError
glue_client = boto3.client('glue')
s3_resource = boto3.resource('s3')

import concurrent.futures as cf
from functools import reduce

import math

# SPARK CONFIG

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.conf.set("spark.sql.jsonGenerator.ignoreNullFields", False)

print('##############TASK-0-IMPORTS+SPARK_CONFIG-COMPLETED################')

#######################################TASK-1#################################################
# PARAMETERS
job_run_id = dt.today().strftime("%Y%m%d%H%M%S%f")
current_day = dt.today().strftime("%Y%m%d")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_STAGE_BUCKET_NAME', 'DATA_TARGET_BUCKET_NAME', 
                                     'FILEPATH_PREFIX', 'MAX_RETRY', 'RETRY_DELAY_IN_SECONDS', 'CONCURRENT_REQUEST_SIZE', 
                                     'COLLECT_SIZE', 'JOB_RUNID', 'DATASET_DATE', 'LIMIT', 'API_URL', 'API_REQ_HEADERS',
                                     'EXPORT_DATE'])

JOB_NAME = args['JOB_NAME']
DATA_STAGE_BUCKET_NAME = args['DATA_STAGE_BUCKET_NAME']
DATA_TARGET_BUCKET_NAME = args['DATA_TARGET_BUCKET_NAME']
FILEPATH_PREFIX = args['FILEPATH_PREFIX']
MAX_RETRY = args['MAX_RETRY']
RETRY_DELAY_IN_SECONDS = args['RETRY_DELAY_IN_SECONDS']
CONCURRENT_REQUEST_SIZE = args['CONCURRENT_REQUEST_SIZE']
COLLECT_SIZE = args['COLLECT_SIZE']
JOB_RUNID = args['JOB_RUNID']
DATASET_DATE = args['DATASET_DATE']
LIMIT = args['LIMIT']
API_URL = args['API_URL']
API_REQ_HEADERS = args['API_REQ_HEADERS']
EXPORT_DATE = args['EXPORT_DATE']

# JOB_NAME = 'jb_msil_mscrm_mosmcall_dl_to_api_outbound_dev'
# DATA_STAGE_BUCKET_NAME = 'msil-inbound-crm-stage-non-prod'
# DATA_TARGET_BUCKET_NAME = 'msil-inbound-crm-outbound-non-prod'
# FILEPATH_PREFIX = 'history/api'
# MAX_RETRY = '1'
# RETRY_DELAY_IN_SECONDS = '2'
# CONCURRENT_REQUEST_SIZE = '20'
# COLLECT_SIZE = '5000'
# JOB_RUNID = ' '
# DATASET_DATE = '20231130'
# LIMIT = '1000'
# API_URL = 'https://4n7ox09647.execute-api.ap-south-1.amazonaws.com/crm-inbound-dev/api/mos/v1/migrate-case'
# API_REQ_HEADERS = '''{"x-api-key": "z93tTjT37r4WYj1kiN3JE4BzHYjd7Fd11ZrFomv5", "Content-Type": "application/json"}'''
# EXPORT_DATE = '1990-01-01'

Client = JOB_NAME.split('_')[1]
Domain = JOB_NAME.split('_')[2]
EntityName = JOB_NAME.split('_')[3]
Source = JOB_NAME.split('_')[4]
Target = JOB_NAME.split('_')[6]
Action = JOB_NAME.split('_')[7]
Env = JOB_NAME.split('_')[8]

if len(JOB_RUNID)==20:
    job_run_id = JOB_RUNID
    current_day = JOB_RUNID[0:8]

database = f"{Client}_{Domain}_{Action}_{Env}"
target_table = f"{EntityName}_{Target}"
target_tablestream = target_table + 'stream'
crawler = f"crawler-{database}-{target_table}"

# SET PATH
stage_bucket = DATA_STAGE_BUCKET_NAME
target_bucket = DATA_TARGET_BUCKET_NAME
filepath_prefix = Env + '/' + FILEPATH_PREFIX
success_prefix = f'{target_table}_success'
fail_prefix = f'{target_table}_fail'

max_retry = int(MAX_RETRY)
retry_delay = int(RETRY_DELAY_IN_SECONDS)
concurrent_request_size = int(CONCURRENT_REQUEST_SIZE)
collect_size = int(COLLECT_SIZE)
limit = int(LIMIT)

filepath_prefix_success = filepath_prefix + '/' + success_prefix + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'
filepath_prefix_fail = filepath_prefix + '/' + fail_prefix + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'

filepath_prefix_combined = filepath_prefix + '/' + target_table + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'
dataframe_write_path = f"s3://{target_bucket}/{filepath_prefix_combined}"
dataframe_stg_path_base = 's3://' + stage_bucket + '/' + Env + '/' + JOB_NAME + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'


raw_filepath_prefix_success = filepath_prefix + '/' + success_prefix + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'
raw_filepath_prefix_fail = filepath_prefix + '/' + fail_prefix + '/' + f'job_run_date={current_day}/job_run_id={job_run_id}'

api_url = API_URL
api_headers = json.loads(API_REQ_HEADERS)
export_date = EXPORT_DATE

# documentdb_uri = "mongodb://docdb-crm-inbound.cluster-caphsxy1o5sy.ap-south-1.docdb.amazonaws.com:27017"
# docdb_options = {
#     "uri": documentdb_uri,
#     "database": "mos-db",
#     "collection": "mos_cases",
#     "username": "mos-rw",
#     "password": "injefn53453",
#     "ssl": "true",
#     "ssl.domain_match": "false",
#     "partitioner": "MongoSamplePartitioner",
#     "partitionerOptions.partitionSizeMB": "10",
#     "partitionerOptions.partitionKey": "_id",
#     "sampleSize": "1000000"
# }

print (f"job_run_id :: {job_run_id}")
print (f"current_day :: {current_day}")

print (f"Client : {Client}")
print (f"Domain : {Domain}")
print (f"EntityName : {EntityName}")
print (f"Source : {Source}")
print (f"Target : {Target}")
print (f"Action : {Action}")
print (f"Env : {Env}")

print (f"database : {database}")
print (f"target_table : {target_table}")
print (f"target_tablestream : {target_tablestream}")
print (f"crawler : {crawler}")

print (f"max_retry : {max_retry}")
print (f"retry_delay : {retry_delay}")

print (f"stage_bucket : {stage_bucket}")
print (f"target_bucket : {target_bucket}")
print (f"filepath_prefix : {filepath_prefix}")
print (f"dataframe_write_path : {dataframe_write_path}")
print (f"dataframe_stg_path_base : {dataframe_stg_path_base}")
print (f"success_prefix : {success_prefix}")
print (f"fail_prefix : {fail_prefix}")
print (f"record limit : {LIMIT}")
print (f"DATASET_DATE : {DATASET_DATE}")

print (f"api_url : {api_url}")
print (f"api_headers : {api_headers}")

print('##############TASK-1-PARAMETERS+SET_PATH-COMPLETED################')

# raise Exception('Forced Exception')

#######################################TASK-2#################################################
# UDF
def uploadFile(inputStream, filePath, bucketName):
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')

    def isBucketExists():
        try:
            s3_resource.meta.client.head_bucket(Bucket=bucketName)
        except botocore.exceptions.ClientError as e:
            return False
        else :
            return True
    #logger  
    if (not isBucketExists()):
        raise Exception("Upload failed. Bucket {} does not exist".format(bucketName))

    obj = s3_resource.Object(bucketName, filePath)
    response = obj.put(Body=inputStream)
    res = response.get("ResponseMetadata")

    if res.get('HTTPStatusCode') == 200:
        #print(f"File uploaded at {filePath}")
        return True
    else :
        #print(f"Upload failed with HTTPStatusCode {res.get('HTTPStatusCode')}")
        return False

def flatten(df):
   # compute Complex Fields (Lists and Structs) in Schema   
    complex_fields = dict([(field.name, field.dataType)
                            for field in df.schema.fields
                            if type(field.dataType) == T.ArrayType or  type(field.dataType) == T.StructType])
    while len(complex_fields)!=0:
        col_name=list(complex_fields.keys())[0]
        # print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))
    
        # if T.StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == T.StructType):
            expanded = [F.col(col_name+'.'+k).alias(k) for k in [ n.name for n in  complex_fields[col_name]]]
            df=df.select("*", *expanded).drop(col_name)
    
        # if T.ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif (type(complex_fields[col_name]) == T.ArrayType):    
            df=df.withColumn(col_name, F.explode_outer(col_name))
    
        # recompute remaining Complex Fields in Schema       
        complex_fields = dict([(field.name, field.dataType)
                                for field in df.schema.fields
                                if type(field.dataType) == T.ArrayType or  type(field.dataType) == T.StructType])
    return df

def get_s3_objects(bucket_name, prefix):
    paths = []
    s3_client = boto3.client("s3")
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        paths = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents']]
        print (response['Marker'])
        print (response['NextMarker'])
    return paths

def get_s3_objects(bucket_name, prefix):
    paths = []
    continuation_token = None
    s3_client = boto3.client("s3")
    
    while True:
        if continuation_token is None:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )

        if "Contents" in response:
            for obj in response['Contents']:
                path = f"s3://{bucket_name}/{obj['Key']}"
                paths.append(path)

        if response.get('IsTruncated', False):
            continuation_token = response['NextContinuationToken']
        else:
            return paths
            break

def getSuccessDataFrame(bucket_name, prefix):
    paths = get_s3_objects(bucket_name, prefix)
    if len(paths)>0:
        df_list = []
        for path in paths:
            df = spark.read.format('json').load(path)
            df_list.append(df)
        df_combined = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
        df_combined = df_combined.coalesce(1)
        return df_combined
    else:
        None

def getFailDataFrame(bucket_name, prefix):
    paths = get_s3_objects(bucket_name, prefix)
    if len(paths)>0:
        df_list = []
        for path in paths:
            df = spark.read.format('json').load(path)
            df_list.append(df)
        df_combined = reduce(lambda df1, df2: df1.unionAll(df2), df_list)
        df_combined = df_combined.coalesce(1)
        return df_combined
    else:
        None

def run_crawler(crawler, database, target_table):
    tables_in_db = [tbl['tableName'] for tbl in spark.sql(f'''show tables in {database}''').select('tableName').collect()]
    response_get = glue_client.get_crawler(Name=crawler)
    state = response_get["Crawler"]["State"]
    try:
        if state != 'READY':
            print(f"Crawler {crawler} is {state.lower()}. Waiting to get ready")
            while (state != "READY") :
                time.sleep(30)
                response_get = glue_client.get_crawler(Name=crawler)
                state = response_get["Crawler"]["State"]
            print(f"Crawler {crawler} is {state.lower()}.")
            response = glue_client.start_crawler(Name=crawler)
            response_get = glue_client.get_crawler(Name=crawler)
            state = response_get["Crawler"]["State"]
            print (f"Crawler '{crawler}' is {state.lower()}.")
            state_previous = state
            if target_table in tables_in_db: spark.sql(f"drop table {database}.{target_table}")
            while (state != "READY") :
                time.sleep(2)
                response_get = glue_client.get_crawler(Name=crawler)
                state = response_get["Crawler"]["State"]
                if state != state_previous:
                    print (f"Crawler {crawler} is {state.lower()}.")
                    state_previous = state

        else:
            if target_table in tables_in_db: spark.sql(f"drop table {database}.{target_table}")
            print(f"Crawler {crawler} is {state.lower()}.")
            response = glue_client.start_crawler(Name=crawler)
            response_get = glue_client.get_crawler(Name=crawler)
            state = response_get["Crawler"]["State"]
            print (f"Crawler {crawler} is {state.lower()}.")
            state_previous = state
            while (state != "READY") :
                time.sleep(2)
                response_get = glue_client.get_crawler(Name=crawler)
                state = response_get["Crawler"]["State"]
                if state != state_previous:
                    print (f"Crawler {crawler} is {state.lower()}.")
                    state_previous = state

    except ClientError as e:
        raise Exception(e)

def s3_load(data_success, data_fail, raw_filepath_prefix_success, raw_filepath_prefix_fail, stage_bucket):
    success_count = len(data_success)
    fail_count = len(data_fail)
    
    if (success_count>0):
        #print(f"{success_count} records send successfully")
        input_stream = bytes(json.dumps(data_success).encode('UTF-8'))
        current_timestamp = dt.today().strftime("%Y-%m-%d %H:%M:%S.%f")
        filepath = f"{raw_filepath_prefix_success}/{current_timestamp}/succeed.json"
        uploadFile(input_stream, filepath, stage_bucket)
        #print (filepath, bucket_target)
        
    if (fail_count>0):
        #print(f"{fail_count} records unable to send")
        input_stream = bytes(json.dumps(data_fail).encode('UTF-8'))
        current_timestamp = dt.today().strftime("%Y-%m-%d %H:%M:%S.%f")
        filepath = f"{raw_filepath_prefix_fail}/{current_timestamp}/failed.json"
        uploadFile(input_stream, filepath, stage_bucket)
        #print (filepath, bucket_target)
    return (success_count, fail_count)

def send_data(batch):
    data_success = []
    data_fail = []
    batch = [payload.encode('UTF-8') for payload in batch]
    def post_data(payload):
        global api_url, api_headers
        payload = json.loads(payload)
        reference_id = payload.pop('reference_id')
        partition_id = payload.pop('pid')
        reference_number = payload['referenceNumber']
        job_type = payload['caseInfo']['jobType']
        created_at = payload['createdAt']
        save_required = payload['saveRequired']
        payload = json.dumps(payload)
        start_datetime = dt.now()
        for retry in range(max_retry):
            url = api_url
            headers = api_headers
            try:
                response = requests.request("POST", url, headers=headers, data=payload)
            except Exception as e:
                class Response():
                    def __init__(self, status_code, json_dict) -> None:
                        self.status_code = status_code
                        self.text = str(json_dict)
                        self.json_dict = json_dict
                    def json(self):
                        return self.json_dict
                exception_message = {'client_exception_handling': str(e)}
                response = Response(999, exception_message)
            response_code = response.status_code
            #response.retry = retry+1
            response.retry = retry
            
            if (response_code>=200 and response_code<300): # or (response_code>=400 and response_code<500):
                break
            #time.sleep(retry_delay*(retry+1)*(retry+1))
            time.sleep(retry_delay*(retry+1))
            #print (f"retry={response.retry} :: response_code={response_code} :: reference_id={reference_id} :: partition_id={partition_id}")
        end_datetime = dt.now()
        response.payload = str(payload)
        response.reference_id = str(reference_id)
        response.reference_number = str(reference_number)
        response.job_type = str(job_type)
        response.reference_id = str(reference_id)
        response.url = str(url)
        response.created_at = str(created_at)
        response.save_required = str(save_required)
        response.start_datetime = start_datetime
        response.end_datetime = end_datetime
        response.record_tsmp = str(end_datetime)
        response.elasped_time_ms = int((end_datetime-start_datetime).microseconds/1000)
        try:
            response.json_data = response.json()
        except Exception as json_error:
            try:
                response.json_data = response.text
            except Exception as text_error:
                response.json_data = str(json_error) + ' + ' + str(text_error)
        try:
            response.target_crm_reference_number = str(response.json()['data']['crmReferenceNumber'])
        except Exception as e:
            response.target_crm_reference_number = f'Cannot Be Parsed Due to Exception -> {str(e)}'
        #time.sleep(0.5)
        return response
    
    with cf.ThreadPoolExecutor() as executor :
        results = executor.map(post_data, batch)
    
    for result in results:
        response_code = result.status_code
        if response_code == 200:
            value = {
                'response_code': str(response_code),
                'record_status': 'SUCCEED',
                'job_type': str(result.job_type),
                'reference_id': str(result.reference_id),
                'reference_number': str(result.reference_number),
                'target_crm_reference_number': str(result.target_crm_reference_number),
                'payload': str(result.payload),
                'response': str(result.json_data),
                'url': str(result.url),
                'start_datetime': str(result.start_datetime),
                'end_datetime': str(result.end_datetime),
                'elasped_time_ms': str(result.elasped_time_ms),
                'retry': str(result.retry),
                'created_at': str(result.created_at),
                'save_required': str(result.save_required),
                'record_tsmp': str(result.record_tsmp)
            }
            data_success.append(value)
            #print (f"response_code: {value['response_code']} :: reference_id: {value[reference_id]} :: reference_number: {value[reference_number]} :: target_crm_reference_number: {value[target_crm_reference_number]} :: retry: {value[retry]}")
        else:
            value = {
                'response_code': str(response_code),
                'record_status': 'FAILED',
                'job_type': str(result.job_type),
                'reference_id': str(result.reference_id),
                'reference_number': str(result.reference_number),
                'target_crm_reference_number': 'NA',
                'payload': str(result.payload),
                'response': str(result.json_data),
                'url': str(result.url),
                'start_datetime': str(result.start_datetime),
                'end_datetime': str(result.end_datetime),
                'elasped_time_ms': str(result.elasped_time_ms),
                'retry': str(result.retry),
                'created_at': str(result.created_at),
                'save_required': str(result.save_required),
                'record_tsmp': str(result.record_tsmp)
            }
            data_fail.append(value)
            #print (f"response_code: {value['response_code']} :: reference_id: {value[reference_id]} :: reference_number: {value[reference_number]} :: target_crm_reference_number: {value[target_crm_reference_number]} :: retry: {value[retry]}")
    return (data_success, data_fail)

def create_delta_lake():
    try :
        schema = T.StructType([
            T.StructField('job_run_id', T.StringType(), False),
            T.StructField('response_code', T.StringType(), False),
            T.StructField('reference_id', T.StringType(), False),
            T.StructField('reference_number', T.StringType(), False),
            T.StructField('target_crm_reference_number', T.StringType(), False),
            T.StructField('created_at', T.StringType(), False),
            T.StructField('save_required', T.StringType(), False),
            T.StructField('payload', T.StringType(), False),
            T.StructField('response', T.StringType(), False),
            T.StructField('record_tsmp', T.StringType(), False)  
        ])
        df_deltalake = spark.createDataFrame([], schema)
        df_deltalake.write.format('delta').save(f's3://msil-inbound-crm-outbound-non-prod/{Env}/history/data-store/{target_tablestream}')
        athena_client = boto3.client('athena')
        query_string = f'''
            CREATE EXTERNAL TABLE {database}.{target_tablestream}
            LOCATION 's3://msil-inbound-crm-outbound-non-prod/{Env}/history/data-store/{target_tablestream}'
            TBLPROPERTIES (
            'table_type'='DELTA'
            )
        '''
        athena_response = athena_client.start_query_execution(QueryString=query_string,
                                                              ResultConfiguration={'OutputLocation': 's3://msil-inbound-crm-tmp/athena/'})
        time.sleep(10)
        result = athena_client.get_query_results(QueryExecutionId=athena_response['QueryExecutionId'])['ResponseMetadata']['HTTPStatusCode']
        if result != 200: 
            raise Exception(f'something went wrong')
    except Exception as e:
        raise Exception(f'Forced Exception due to {e}')

print('##############TASK-2-UDF-DEFINED################')
# raise Exception('Forced Exception')

###################################TASK-3-DATA-TRANSFORMATION#######################################
if len(JOB_RUNID)!=20:
    # dyf_docdb = glueContext.create_dynamic_frame.from_options(connection_type="documentdb", connection_options=docdb_options)
    # df_docdb = dyf_docdb.toDF()
    # df_docdb.createOrReplaceTempView('docdb')
    
    # df_migrate = spark.sql(f'''
    #                           select
    #                               moscasenumbermain
    #                           from
    #                               msil_mscrm_structured_dev.{EntityName}_dl
    #                           where
    #                               dataset_date = '{DATASET_DATE}'
    #                           ''')
    # df_migrated = spark.sql(f'''
    #                           select
    #                               moscasenumbermain 
    #                           from
    #                               msil_mscrm_structured_dev.{EntityName}_dl
    #                           where
    #                               createdat>=to_timestamp('{export_date}')
    #                               and concat('0001-', moscasenumbermain) in (select coalesce(docdb.referenceNumber, '') from docdb)
    #                               and dataset_date = '{DATASET_DATE}'
    #                           ''')
    # df_mos_dl = spark.sql(f'''
    #                           select
    #                               *
    #                           from 
    #                                 msil_mscrm_structured_dev.{EntityName}_dl
    #                           where
    #                               createdat>=to_timestamp('{export_date}')
    #                               and concat('0001-', moscasenumbermain) not in (select coalesce(docdb.referenceNumber, '') from docdb)
    #                               and dataset_date = '{DATASET_DATE}'
    #                           ''').limit(limit)
    
    df_migrate = spark.sql(f'''
                           select
                              moscasenumbermain
                           from
                              msil_mscrm_structured_dev.{EntityName}_dl
                           where
                              dataset_date = '{DATASET_DATE}'
                              and createdat>=to_timestamp('{export_date}')
                           ''')

    if target_tablestream in [rw['tableName'] for rw in spark.sql(f"show tables in {database}").collect()]:
        df_migrated = spark.sql(f'''
                               select
                                  reference_number 
                               from
                                  {database}.{target_tablestream}
                               where
                                  response_code = '200'
                                  and reference_id in (select
                                                          reference_id
                                                       from
                                                            msil_mscrm_structured_dev.{EntityName}_dl where dataset_date = '{DATASET_DATE}'
                                                                                                            and createdat>=to_timestamp('{export_date}'))
                               ''')
        df_mos_dl = spark.sql(f'''
                                   select
                                      *
                                   from 
                                        msil_mscrm_structured_dev.{EntityName}_dl
                                   where
                                      --concat('1015-', moscasenumbermain) not in (select reference_number from {database}.{target_tablestream} where response_code = '200')
                                      moscasenumbermain not in (select reference_number from {database}.{target_tablestream} where response_code = '200')
                                      and dataset_date = '{DATASET_DATE}'
                                      and createdat>=to_timestamp('{export_date}')
                                   ''').limit(limit)
    else:
        #df_migrated = spark.createDataFrame([], '')
        create_delta_lake()
        df_migrated = spark.sql(f'''select * from {database}.{target_tablestream}''')
        df_mos_dl = spark.sql(f'''
                                   select
                                      *
                                   from 
                                        msil_mscrm_structured_dev.{EntityName}_dl
                                   where
                                      dataset_date = '{DATASET_DATE}'
                                      and createdat>=to_timestamp('{export_date}')
                                   ''').limit(limit)
    
    df_mosflup_dl = spark.sql(f'''
                               select
                                  *
                               from 
                                    msil_mscrm_structured_dev.{EntityName}followup_dl
                               where
                                  dataset_date = '{DATASET_DATE}'
                               ''')
    
    df_mosescln_dl = spark.sql(f'''
                               select
                                  *
                               from 
                                    msil_mscrm_structured_dev.{EntityName}escalation_dl
                               where
                                  dataset_date = '{DATASET_DATE}'
                               ''')
    
    migrate_count = df_migrate.count()
    records_migrated_count = df_migrated.count()
    current_migration_count = df_mos_dl.count()
    
    print (f"total records need to migrate={migrate_count}\nrecords already migrated={records_migrated_count}\ncurrent migration count {current_migration_count}")
    
    df_mos_dl.createOrReplaceTempView('mos_dl')
    df_mosflup_dl.createOrReplaceTempView('mosflup_dl')
    df_mosescln_dl.createOrReplaceTempView('mosescln_dl')
    records_to_be_migrated = df_mos_dl.count()
    if (records_to_be_migrated>0):
        print (f"count of records to be migrated : {records_to_be_migrated}")
    else:
        raise Exception ("No records to be migrated")
    
    df_mosescln_dl_grp = spark.sql(f'''
    select
        reference_id,
        struct(
            date_format(createdon_escalation, 'yyyy-MM-dd HH:mm:ss.SSS') createdOn,
            type_escalation type,
            deliverystatus_escalation deliveryStatus,
            requestid_escalation requestId,
            --requestTarget requestTarget, TBD
            workshopname_escalation workShopName,
            workshopcode_escalation workShopCode,
            workshopcity_escalation workShopCity,
            workshopcity_escalation workShopRegion,
            struct(
                subject_escalation subject,
                mobilenumber_escalation mobileNumber,
                name_escalation name,
                template_escalation template,
                designation_escalation designation
            ) smsNotification
            
        ) escln_data
    from
        mosescln_dl
    ''').groupBy('reference_id').agg(F.expr("collect_list(escln_data) notificationLogs"))
    df_mosescln_dl_grp.createOrReplaceTempView('escln_dl')

    df_mosflup_dl_grp = spark.sql(f'''
    select
        reference_id,
        struct(
            callOriginValue callOriginValue,
            callOriginType callOriginType,
            callDirection callDirection,
            callPurpose callPurpose,
            phoneNumber_followup phoneNumber,
            sendSMS sendSMS,
            repeatCall_followup repeatCall,
            callStatus_followup callStatus,
            disposition_followup disposition,
            crmCaseStatus crmCaseStatus,
            crmCaseStatusCode crmCaseStatusCode,
            nextFollowUpTime nextFollowUpTimes,
            struct(
                dealerName dealerName,
                mobileNumber mobileNumber,
                forCode forCode,
                mulCode mulCode,
                locCode locCode,
                mapCode mapCode,
                outletCode outletCode,
                dealerCode dealerCode,
                cityCd_main cityCd,
                cityDesc_main cityDesc,
                regionCd_main regionCd,
                regionDesc_main regionDesc,
                statecd_main stateCd,
                stateDesc_main stateDesc,
                moscategory_main mosCategory,
                dealeruniquecode dealerUniqueCd
            ) dealerInfo,
            struct(
                dealerUniqueCode dealerUniqueCode,
                dealerName agencyName,
                mobileNumber mobileNumber,
                cityDesc_main cityDesc,
                regionDesc_main regionDesc,
                stateDesc_main stateDesc
            ) towingAgencyInfo,
            remarks_followup remarks,
            allocationType allocationType,
            adviceMessage adviceMessage,
            customerConsent customerConsent,
            name_followup name,
            followupCreatedBy followupCreatedBy,
            followupCreatedByName followupCreatedByName,
            agentName_followup agentName,
            agentId_followup agentId,
            date_format(createdat_followup, 'yyyy-MM-dd HH:mm:ss.SSS') createdAt,
            createdBy_followup createdBy,
            createdByName_followup createdByName
        ) flup_data
    from
        mosflup_dl
    ''').groupBy('reference_id').agg(F.expr("collect_list(flup_data) followUps"))
    df_mosflup_dl_grp.createOrReplaceTempView('flup_dl')

    df_mos_api_structure = spark.sql('''
    select
        case
            when createdat>=to_timestamp('2023-01-01') then 'Y'
            else 'N'
        end saveRequired,
        mos_dl.reference_id reference_id,
        --concat('1015-', moscasenumbermain) referenceNumber,
        moscasenumbermain referenceNumber,
        caseStage caseStage,
        crmCaseStatusCode crmCaseStatusCode,
        crmCaseStatus crmCaseStatus,
        dtcCodes dtcCodes,
        caseSFDCId caseSFDCId,
        syncServiceDisplayValue syncServiceDisplayValue,
        primaryuseraction_main primaryUserAction,
        eventStatus eventStatus,
        vehicleSpeed vehicleSpeed,
        array(
            struct(
                notificationReceiveDateTime notificationReceiveDateTime,
                notificationMessage  notificationMessage,
                notificationDateTime notificationDateTime,
                notificationWarningImage notificationWarningImage,
                notificationWarningLight notificationWarningLight,
                notificationOccurDateTime notificationOccurDateTime,
                notificationWarningName notificationWarningName,
                notificationWarningCode notificationWarningCode
            )
        ) notificationList,
        eventType eventType,
        requestGeneratedFrom requestGeneratedFrom,
        ends ends,
        status status,
        lastSyncSource lastSyncSource,
        stateDontUse stateDontUse,
        starts starts,
        description_main description,
        eventTriggerInfo eventTriggerInfo,
        tripDistance tripDistance,
        fuelLevelPetrol fuelLevelPetrol,
        array(
            struct(
                question question,
                answer answer,
                inquiryId inquiryId
            )
        ) additionalInterview,
        technicianid_sec technicianId,
        requestGeneratedFromInternalId requestGeneratedFromInternalId,
        feCaseNumber feCaseNumber,
        tripDuration tripDuration,
        array(
            struct (
                warningCode warningCode,
                descriptionWarningName descriptionWarningName,
                warningLight warningLight,
                receiveDateTime receiveDateTime,
                controllerName controllerName,
                dtcdescription description,
                occurDateTime occurDateTime,
                warningCodeImage warningCodeImage,
                dtcCodes dtcCodes,
                dtcName dtcName,
                drivableLevel drivableLevel,
                warningName warningName
            )
        ) dtcList,
        repeatCall repeatCall,
        dtcDescription dtcDescription,
        sequenceNo sequenceNo,
        technicianContactNo_sec technicianContactNo,
        requestGeneratedFromDisplayValue requestGeneratedFromDisplayValue,
        syncServiceInternalId syncServiceInternalId,
        remarks remarks,
        array(
            struct(
            chartStatus chartStatus,
            requestNumber requestNumber,
            decisionDateTime decisionDateTime,
            memo memo,
            decisionResult decisionResult,
            adviceMessage adviceMessage,
            operationResult operationResult
            )
        ) chartInfo,
        locationCaptured locationCaptured,
        priority priority,
        mosCaseSourceInternalId mosCaseSourceInternalId,
        date_format(googleetatime, 'yyyy-MM-dd HH:mm:ss.SSS') googleEtaTime,
        connectToCallCenter connectToCallCenter,
        secondaryUserName secondaryUserName,
        array(
            struct (
            eventTriggerInfo eventTriggerInfo,
            primaryuseraction_main primaryUserAction,
            date_format(eventdate, 'yyyy-MM-dd') eventDate,
            date_format(createdat, 'yyyy-MM-dd HH:mm:ss.SSS') createdOn
            )
        ) eventDetails,
        workName workName,
        date_format(eventdate, 'yyyy-MM-dd HH:mm:ss.SSS') eventDate,
        pleaseSelectYourLocation pleaseSelectYourLocation,
        trackingLink trackingLink,
        batteryVoltage batteryVoltage,
        adviceMessage adviceMessage,
        syncService syncService,
        createdBy createdBy,
        createdByName createdByName,
        createdById createdById,
        lastUpdateSource lastUpdateSource,
        lastServiceTypeMOS lastServiceTypeMOS,
        fuelLevelCNG fuelLevelCNG,
        failSafe failSafe,
        caseOriginSource caseOriginSource,
        caseModifiedSource caseModifiedSource,
        modifiedBy modifiedBy,
        modifiedByName modifiedByName,
        agentName agentName,
        agentId agentId,
        date_format(createdat, 'yyyy-MM-dd HH:mm:ss.SSS') createdAt,
        date_format(modifiedat, 'yyyy-MM-dd HH:mm:ss.SSS') modifiedAt,
        allocationType allocationType,
        struct(
            dealerUniqueCode dealerUniqueCode,
            dealerName agencyName,
            mobileNumber mobileNumber,
            citydesc_main cityDesc,
            regiondesc_main regionDesc,
            statedesc_main stateDesc
        ) towingAllocatedAgency,
        struct(
            registeredContactNo registeredContactNo,
            customerMobile customerMobile,
            vehicleownername_main vehicleOwnerName,
            state state,
            stateName stateName,
            city city,
            cityKey cityKey,
            region_main region,
            area area,
            longitude_main longitude,
            latitude_main latitude,
            zone_main zone,
            country country,
            zoneDisplayValue zoneDisplayValue,
            registeredcontactno registeredContact,
            regionDisplayValue regionDisplayValue,
            customer customer,
            street street,
            cityKeyInternalId cityKeyInternalId,
            phoneNumber phoneNumber,
            cityKeyDisplayValue cityKeyDisplayValue,
            regionInternalId regionInternalId,
            secondaryUserContactNo secondaryUserContactNo,
            zoneInternalId zoneInternalId,
            pincode pincode,
            addressSameAsCustomer addressSameAsCustomer
        ) customerInfo,
        struct(
            vehicleRegNo vehicleRegNo,
            modeldesc_super model,
            variantdesc_super variantDesc,
            colordesc_super color,
            date_format(saledate, 'yyyy-MM-dd') saleDate,
            warrantyStatus warrantyStatus,
            date_format(warrantyexpirydate, 'yyyy-MM-dd') warrantyExpiryDate,
            correctedColor correctedColor,
            correctedVariant correctedVariant,
            correctedWarrantyStatus correctedWarrantyStatus,
            correctedSalesDate correctedSalesDate,
            correctedModel correctedModel,
            correctedExpiryDate correctedExpiryDate,
            secLastOdometerReading secLastOdometerReading,
            lastServiceDealer lastServiceDealer,
            salesDealer salesDealer,
            engineRunningStatus engineRunningStatus,
            secondLastServiceDealer secondLastServiceDealer,
            lastOdometerReading lastOdometerReading,
            odometerReading odometerReading,
            secLastServiceType secLastServiceType,
            ignitionStatus ignitionStatus,
            modelcd_super modelCode,
            colorcd_super colorCode,
            variantcd_super variantCode,
            channel_super channel
        ) vehicleInfo,
        struct(
            mosCaseSource mosCaseSource,
            jobType jobType,
            familyInvolvement familyInvolvement,
            problemKey problemKey,
            problemNature problemNature,
            problemDescription problemDescription,
            date_format(allocationtime, 'yyyy-MM-dd HH:mm:ss.SSS') allocationTime,
            custCancelReason custCancelReason,
            date_format(eta, 'yyyy-MM-dd HH:mm:ss.SSS') eta,
            date_format(compregistertime, 'yyyy-MM-dd HH:mm:ss.SSS') compRegisterTime,
            chargesTaken chargesTaken,
            techCancelReason techCancelReason,
            syncToSpoors syncToSpoors,
            caseType caseType,
            currentLocation currentLocation,
            landmark landmark,
            technicianProblemKey technicianProblemKey,
            technicianProblemDescription technicianProblemDescription,
            jobTypeDisplayValue jobTypeDisplayValue,
            mCallOdometerReading mCallOdometerReading,
            jobTypeInternalId jobTypeInternalId,
            assistanceRequired assistanceRequired,
            complaintNumber complaintNumber,
            enteredDistance enteredDistance,
            customerConsent customerConsent,
            technicianVehicleType technicianVehicleType,
            technicianContactNo technicianContactNo,
            breakdownCity breakdownCity,
            problemDescriptionDisplayValue problemDescriptionDisplayValue,
            problemDescriptionInternalId problemDescriptionInternalId,
            distance distance,
            mosCaseSourceDisplayValue mosCaseSourceDisplayValue,
            towingType towingType,
            familyInvolvementDisplayValue familyInvolvementDisplayValue,
            familyInvolvementInternalId familyInvolvementInternalId,
            towingAssistanceConsent towingAssistanceConsent,
            towingAssistanceStatus towingAssistanceStatus,
            towingAssistanceStatusCode towingAssistanceStatusCode,
            serviceGivenByTowingAgency serviceGivenByTowingAgency,
            reasonForServiceNotProvided reasonForServiceNotProvided,
            serviceArrangedByDealer serviceArrangedByDealer,
            serviceNotReqByCust serviceNotReqByCust,
            customerFeedback customerFeedback,
            feedbackReason feedbackReason,
            towingCancellationReason towingCancellationReason,
            jobCode jobCode,
            date_format(outforservicetime, 'yyyy-MM-dd HH:mm:ss.SSS') outForServiceTime,
            date_format(reachedtime, 'yyyy-MM-dd HH:mm:ss.SSS') reachedTime,
            date_format(resolvedtime, 'yyyy-MM-dd HH:mm:ss.SSS') resolvedTime,
            allocationTAT allocationTAT,
            outForServiceTAT outForServiceTAT,
            reachTAT reachTAT,
            nextFollowUpTime nextFollowUpTime,
            lastFollowUpTime lastFollowUpTime,
            lastFollowUpRemarks lastFollowUpRemarks,
            caseEscalationStatus caseEscalationStatus,
            flagAtAllocationDealer flagAtAllocationDealer,
            caseStatusTransTime caseStatusTransTime,
            date_format(towingoutforservicetime, 'yyyy-MM-dd HH:mm:ss.SSS') towingOutForServiceTime,
            date_format(towingreachedbreakdownlocationtime, 'yyyy-MM-dd HH:mm:ss.SSS') towingReachedBreakdownLocationTime,
            date_format(towingreachedworkshoptime, 'yyyy-MM-dd HH:mm:ss.SSS') towingReachedWorkshopTime,
            towingPendingOFSTime towingPendingOFSTime
        ) caseInfo,
        struct(
            dealerName dealerName,
            mobileNumber mobileNumber,
            forCode forCode,
            mulCode mulCode,
            locCode locCode,
            mapCode mapCode,
            outletCode outletCode,
            dealerCode dealerCode,
            cityCd_main cityCd,
            cityDesc_main cityDesc,
            regionCd_main regionCd,
            regionDesc_main regionDesc,
            statecd_main stateCd,
            stateDesc_main stateDesc,
            moscategory_main mosCategory,
            dealeruniquecode dealerUniqueCd
        ) dealerInfo,
        dummyDealer dummyDealer,
        struct(
            cityCd cityCd,
            towingdestinationcitydesc cityDesc,
            dealer_name dealerName,
            towingdestinationdealeruniquecode dealerUniqueCd,
            forcd forCode,
            loccd locCode,
            mapcd mapCode,
            phone mobileNumber,
            mosCategory_main mosCategory,
            mulcd mulCode,
            outletcd outletCode,
            towingdestinationdealercode dealerCode,
            regionCd regionCd,
            region regionDesc,
            stateCd stateCd,
            towingdestinationstatedesc stateDesc
        ) towingDestinationDealer,
        struct(
            cityCd_main cityCd,
            cityDesc_main cityDesc,
            dealerName dealerName,
            dealerUniqueCode dealerUniqueCd,
            forCode forCode,
            locCode locCode,
            mapCode mapCode,
            mobileNumber mobileNumber,
            mosCategory_main mosCategory,
            mulCode mulCode,
            outletCode outletCode,
            dealerCode dealerCode,
            regionCd_main regionCd,
            regionDesc_main regionDesc,
            statecd_main stateCd,
            stateDesc_main stateDesc
        ) towingAllocatedDealer,
        struct(
            dummyDealerUniqueCode dealerUniqueCode,
            uniqueCodeFarEye uniqueCodeFarEye,
            dummyDealerName dealerName,
            dummyDealerState state,
            dummyDealerCity city
        ) dummyDealerInformation,
        array(
            struct(
                adviceMessage adviceMessage,
                chartStatus chartStatus,
                decisionDateTime decisionDateTime,
                decisionResult decisionResult,
                memo memo,
                operationResult operationResult,
                requestNumber requestNumber
            )
        ) drivableAdviceSummary,
        caseStageDisplayValue caseStageDisplayValue,
        caseStageInternalId caseStageInternalId,
        runSheetNo runSheetNo,
        customerConsent customerConsent,
        customerConsentDisplayValue customerConsentDisplayValue,
        receiveDateTime receiveDateTime,
        memo memo,
        newText newText,
        googleTrackingLink googleTrackingLink,
        autoGenerate autoGenerate,
        secondaryUserContactNo_sec secondaryUserContactNo,
        locality locality,
        followUps followUps,
        notificationLogs notificationLogs
    from
        mos_dl
        left join flup_dl on (flup_dl.reference_id=mos_dl.reference_id)
        left join escln_dl on (escln_dl.reference_id=mos_dl.reference_id)
    ''')

    df_mos_api_structure.write.format('parquet').mode('overwrite').save(dataframe_stg_path_base+'/'+'df_mos_api_structure')
    df_mos_api_structure = spark.read.format('parquet').load(dataframe_stg_path_base+'/'+'df_mos_api_structure').cache()

    df_arr = {
            'df_mos_api_structure' : df_mos_api_structure
        }

    total_count = 0
    for frame in df_arr:
        count = df_arr[frame].count()
        df_arr[frame].cache()
        total_count = total_count + count
        print (f"{frame} :: {count}")
        
    df_mos_api_structure_count = df_mos_api_structure.count()
        
    if df_mos_api_structure_count != total_count:
        print ("Invalid Bi-Furcation")
    else:
        print ("Valid Bi-Furcation")

print('##############TASK-3-DATA-TRANSFORMATION-COMPLETED################')
# raise Exception('Forced Exception - Trial Run') #remove

###################################TASK-4-DATA-LOAD#######################################
if len(JOB_RUNID)!=20:
    for frame in df_arr:
        requests_pushed_count = 0
        requests_succeed_count = 0
        requests_failed_count = 0
        df = df_arr[frame]
        # df = df.limit(limit)
        df_count = df.count()
        partitions = math.ceil(df_count/collect_size)
        if partitions==0: partitions=1
        df = df.repartition(partitions).withColumn('pid', F.spark_partition_id())
        for pid in range(partitions):
            pid_start_time = time.time()
            pid_data_success = []
            pid_data_fail = []
            cid_elasped_times = []
            dfn = df.filter(f"pid={pid}")
            record_count = dfn.count()
            data = dfn.toJSON().collect()
            for size in range(0, record_count, concurrent_request_size):
                batch_data = data[size:size+concurrent_request_size]
                cid_start_time = time.time()
                data_success, data_fail = send_data(batch_data)
                cid_end_time = time.time()
                pid_data_success = pid_data_success + data_success
                pid_data_fail = pid_data_fail + data_fail
                cid_elasped_time = cid_end_time-cid_start_time
                cid_elasped_times.append(cid_elasped_time)
                micro_batch = [{'response_code': data_element['response_code'],
                                'reference_id': data_element['reference_id'],
                                'reference_number': data_element['reference_number'],
                                'target_crm_reference_number': data_element['target_crm_reference_number'],
                                'response_code': data_element['response_code'],
                                'job_run_id': job_run_id,
                                'created_at': data_element['created_at'],
                                'save_required': data_element['save_required'],
                                'record_tsmp': data_element['record_tsmp'],
                                'payload': data_element['payload'],
                                'response': data_element['response']
                                } for data_element in data_success + data_fail]
                df_micro_batch = spark.createDataFrame(micro_batch).select('job_run_id', 'response_code', 'reference_id', 'reference_number', 'target_crm_reference_number', 'created_at', 'save_required', 'payload', 'response', 'record_tsmp')
                #df_micro_batch.repartition(1).write.mode('append').saveAsTable(f'{database}.{target_tablestream}')
                df_micro_batch.repartition(1).write.format('delta').mode('append').save(f's3://msil-inbound-crm-outbound-non-prod/{Env}/history/data-store/{target_tablestream}')
            pid_success_count, pid_fail_count = s3_load(pid_data_success, pid_data_fail, raw_filepath_prefix_success, raw_filepath_prefix_fail, stage_bucket)
            requests_pushed_count = requests_pushed_count + record_count
            requests_succeed_count = requests_succeed_count + pid_success_count
            requests_failed_count = requests_failed_count + pid_fail_count
            pid_end_time = time.time()
            pid_elasped_time = int(pid_end_time-pid_start_time)
            print (f"{frame}_Total_Recs->{df_count} : Rqs_Psd->{requests_pushed_count} : Scc_Rqs->{requests_succeed_count} : Fl_Rqs->{requests_failed_count} : pid_el_tm_s->{pid_elasped_time} : cid_el_tm_ms->mx={int(max(cid_elasped_times)*1000)}, mn={int(min(cid_elasped_times)*1000)}")
            if (pid_elasped_time>20) and (requests_pushed_count<df_count):
                time.sleep(1)
                sleep_time_s = min((pid_elasped_time-1), 30)
                print (f"cooling off -> wait time:{sleep_time_s}")
                time.sleep(sleep_time_s)
                print ("started")

print (F'JSON->{stage_bucket}->{filepath_prefix_success}\nPARQUET->{dataframe_write_path}')
df_success = getSuccessDataFrame(stage_bucket, filepath_prefix_success)
df_fail = getFailDataFrame(stage_bucket, filepath_prefix_fail)

if (df_fail != None) and (df_success != None):
    df_api_response = df_success.unionAll(df_fail)
elif (df_fail == None) and (df_success == None):
    df_api_response = None
elif (df_fail == None) and (df_success != None):
    df_api_response = df_success
elif (df_fail != None) and (df_success == None):
    df_api_response = df_fail

df_api_response = df_api_response.select('record_status', 'job_type', 'reference_id', 'reference_number', 
                                         'target_crm_reference_number', 'response_code', 'response', 
                                         'elasped_time_ms', 'payload', 'url', 'start_datetime', 'end_datetime', 'retry')

if len(JOB_RUNID)!=20:
    df_api_response.write.format('parquet').save(dataframe_write_path)
else:
    df_api_response.write.format('parquet').mode('overwrite').save(dataframe_write_path)

print('##############TASK-4-DATA-LOAD-COMPLETED################')

###################################TASK-7-REFRESH-ATHENA#######################################

run_crawler(crawler=crawler, database=database, target_table=target_table)

print('##############TASK-7-REFRESH-ATHENA-COMPLETED################')

print('##############JOB-COMPLETED-SUCCESSFULLY################')
job.commit()

import subprocess

# Define the requirements
requirements = ["pandas", "numpy"]  # Add your libraries here

# Directory to save the wheel files
output_dir = "/tmp/wheels"

# Create the directory
subprocess.run(["mkdir", "-p", output_dir], check=True)

# Use pip download to fetch compatible wheels
for package in requirements:
    subprocess.run(["pip", "download", package, "--dest", output_dir], check=True)

# Optionally, list downloaded files
downloaded_files = subprocess.check_output(["ls", output_dir]).decode("utf-8").split("\n")
print("Downloaded files:", downloaded_files)

import boto3
import os

s3_client = boto3.client('s3')
bucket_name = "your-s3-bucket"
s3_folder = "compatible-wheels/"

for file in downloaded_files:
    if file:  # Skip empty lines
        file_path = os.path.join(output_dir, file)
        s3_client.upload_file(file_path, bucket_name, s3_folder + file)
        print(f"Uploaded {file} to S3.")

import subprocess
import os
import boto3

# Define the temporary directory for installed files
install_dir = "/tmp/python_packages"

# Create the directory
os.makedirs(install_dir, exist_ok=True)

# Define the requirements
requirements = ["pandas", "numpy"]  # Add your dependencies here

# Install packages into the temporary directory
for package in requirements:
    subprocess.run([
        "pip", "install", package, 
        "--target", install_dir
    ], check=True)

# Optional: List installed packages
installed_packages = os.listdir(install_dir)
print("Installed packages:", installed_packages)

# Zip the installed packages for easy transfer
zip_file = "/tmp/python_packages.zip"
subprocess.run(["zip", "-r", zip_file, install_dir], check=True)

# Upload the zip file to S3
s3_client = boto3.client('s3')
bucket_name = "your-s3-bucket"
s3_key = "compatible-packages/python_packages.zip"

s3_client.upload_file(zip_file, bucket_name, s3_key)
print(f"Uploaded {zip_file} to S3 as {s3_key}")

def create_delta_lake():
    try :
        schema = T.StructType([
            T.StructField('job_run_id', T.StringType(), False),
            T.StructField('response_code', T.StringType(), False),
            T.StructField('reference_id', T.StringType(), False),
            T.StructField('reference_number', T.StringType(), False),
            T.StructField('target_crm_reference_number', T.StringType(), False),
            T.StructField('created_at', T.StringType(), False),
            T.StructField('save_required', T.StringType(), False),
            T.StructField('payload', T.StringType(), False),
            T.StructField('response', T.StringType(), False),
            T.StructField('record_tsmp', T.StringType(), False)  
        ])
        df_deltalake = spark.createDataFrame([], schema)
        df_deltalake.write.format('delta').save(f's3://msil-inbound-crm-outbound-non-prod/{Env}/history/data-store/{target_tablestream}')
        athena_client = boto3.client('athena')
        query_string = f'''
            CREATE EXTERNAL TABLE {database}.{target_tablestream}
            LOCATION 's3://msil-inbound-crm-outbound-non-prod/{Env}/history/data-store/{target_tablestream}'
            TBLPROPERTIES (
            'table_type'='DELTA'
            )
        '''
        athena_response = athena_client.start_query_execution(QueryString=query_string,
                                                              ResultConfiguration={'OutputLocation': 's3://msil-inbound-crm-tmp/athena/'})
        time.sleep(10)
        result = athena_client.get_query_results(QueryExecutionId=athena_response['QueryExecutionId'])['ResponseMetadata']['HTTPStatusCode']
        if result != 200: 
            raise Exception(f'something went wrong')
    except Exception as e:
        raise Exception(f'Forced Exception due to {e}')

https://dbc-f46ce310-2494.cloud.databricks.com/settings/user/developer?o=3445049866680796


import urllib.parse
from datetime import datetime, timedelta
from itertools import product
import requests


def generate_date_range(start, end):
    """Generate a list of dates between start and end (inclusive)."""
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)


def build_primary_combinations(primary_filters):
    """Generate all combinations of primary filters."""
    filter_values = []
    for primary_filter in primary_filters:
        field = primary_filter["field"]
        operator = primary_filter["operator"]

        if primary_filter["type"] == "range":
            # Generate individual values for range
            values = [f"{field} {operator} '{date}'" for date in generate_date_range(primary_filter["start"], primary_filter["end"])]
        
        elif primary_filter["type"] == "list":
            # Generate individual values for list
            values = [f"{field} {operator} '{value}'" for value in primary_filter["values"]]
        
        else:
            raise ValueError(f"Unsupported filter type: {primary_filter['type']}")

        filter_values.append(values)

    # Generate all combinations of primary filters
    return list(product(*filter_values))


def build_secondary_clause(secondary_filters):
    """Combine all secondary filters into a single clause."""
    clauses = []
    for secondary_filter in secondary_filters:
        field = secondary_filter["field"]
        operator = secondary_filter["operator"]
        value = secondary_filter["value"]
        clause = f"{field} {operator} '{value}'"
        clauses.append(clause)
    return " and ".join(clauses)


def build_prepared_requests(base_url, config, auth=None, headers=None):
    """Generate all prepared requests based on primary and secondary filters."""
    prepared_requests = []
    primary_filters = config.get("primary_filters", [])
    secondary_filters = config.get("secondary_filters", [])

    # Handle empty filters
    if not primary_filters and not secondary_filters:
        # No filters, return base request
        req = requests.Request("GET", base_url, headers=headers, auth=auth)
        prepared_requests.append(req.prepare())
        return prepared_requests

    # Generate combinations of primary filters
    primary_combinations = build_primary_combinations(primary_filters) if primary_filters else [[]]

    # Build the secondary filter clause
    secondary_clause = build_secondary_clause(secondary_filters) if secondary_filters else ""

    # Combine primary combinations with secondary filters
    for primary_combination in primary_combinations:
        if primary_combination:
            primary_clause = " and ".join(primary_combination)
        else:
            primary_clause = ""

        if primary_clause and secondary_clause:
            full_query = f"{primary_clause} and {secondary_clause}"
        elif primary_clause:
            full_query = primary_clause
        elif secondary_clause:
            full_query = secondary_clause
        else:
            full_query = ""  # No filters at all

        if full_query:
            query_url = f"{base_url}?$filter={urllib.parse.quote(full_query)}"
        else:
            query_url = base_url

        # Create the request
        req = requests.Request("GET", query_url, headers=headers, auth=auth)
        prepared_requests.append(req.prepare())

    return prepared_requests



# Example Usage
base_url = "https://example.com/odata"
filter_config = {
    "primary_filters": [
        {
            "field": "todate",
            "type": "range",
            "operator": "eq",
            "start": "2024-12-01",
            "end": "2024-12-02"
        },
        {
            "field": "fromdate",
            "type": "range",
            "operator": "eq",
            "start": "2024-12-01",
            "end": "2024-12-02"
        }
    ],
    "secondary_filters": [
        {
            "field": "Status",
            "type": "single",
            "operator": "eq",
            "value": "Active"
        },
        {
            "field": "Type",
            "type": "single",
            "operator": "ne",
            "value": "Closed"
        }
    ]
}

requests = build_requests(base_url, filter_config)
for req in requests:
    print(req)


from urllib.parse import urlparse, parse_qs
import os
import itertools
from requests import Request, Session

def create_request_urls(base_url, primary_filters, secondary_filters, headers=None, auth=None):
    """
    Generate prepared requests based on primary and secondary filters.

    Args:
        base_url (str): The base API URL.
        primary_filters (list): List of primary filter dictionaries.
        secondary_filters (list): List of secondary filter dictionaries.
        headers (dict): Optional headers for the request.
        auth (tuple): Optional basic authentication (username, password).

    Returns:
        list: A list of prepared requests.
    """
    session = Session()
    request_urls = []

    # Generate combinations of primary filter criteria
    primary_combinations = []
    for filter_ in primary_filters:
        field = filter_["field"]
        if filter_["type"] == "range":
            # Generate range values (e.g., dates)
            start = filter_["start"]
            end = filter_["end"]
            primary_combinations.append([(field, start), (field, end)])
        elif filter_["type"] == "list":
            # Use list values directly
            primary_combinations.append([(field, value) for value in filter_["values"]])
        elif filter_["type"] == "single":
            primary_combinations.append([(field, filter_["value"])])

    # Cartesian product of primary filter combinations
    primary_combinations = list(itertools.product(*primary_combinations))

    # Safeguard against large number of combinations
    if len(primary_combinations) > 10000:
        raise ValueError("Too many primary filter combinations, consider reducing the input size.")

    for combination in primary_combinations:
        query_params = {}
        for k, v in combination:
            if k in query_params:
                raise ValueError(f"Duplicate key detected in combination: {k}")
            query_params[k] = v

        # Add secondary filters to query params
        for filter_ in secondary_filters:
            field = filter_["field"]
            if filter_["type"] == "single":
                query_params[field] = filter_["value"]

        # Check query string length to avoid HTTP 414 errors
        query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
        if len(query_string) > 2000:  # Example threshold, adjust as needed
            raise ValueError("Query string too long, consider splitting the request.")

        # Prepare the URL
        req = Request(
            "GET",
            base_url,
            params=query_params,
            headers=headers,
            auth=auth
        )
        prepared = session.prepare_request(req)
        request_urls.append(prepared)

    return request_urls

def generate_pyspark_partition_path(base_path, request_url, primary_filters):
    """
    Generate a PySpark-like partition path using primary filters from the request URL.

    Args:
        base_path (str): Base path or prefix for the partition path.
        request_url (str): The request URL containing query parameters.
        primary_filters (list): List of primary filter dictionaries.

    Returns:
        str: A partition path based on primary filters.
    """
    # Parse the URL to extract query parameters
    parsed_url = urlparse(request_url)
    query_params = parse_qs(parsed_url.query)  # Returns a dictionary of query params

    # Only use primary filters to construct the path
    partition_segments = []
    for filter_ in primary_filters:
        field = filter_["field"]
        if field in query_params:
            values = query_params.get(field, [])  # Safely retrieve the field values
            for value in values:
                partition_segments.append(f"{field.lower()}={str(value).lower()}")
        else:
            print (query_params)
            raise KeyError(f"Expected primary filter field '{field}' not found in query parameters.")

    # Join the segments to create a partition path
    partition_path = base_path + '/' + '/'.join(partition_segments)
    return partition_path

# Example usage
base_url = "https://example.com/odata"
base_path = "s3://data-lake"
headers = {"Authorization": "Bearer YOUR_TOKEN"}
primary_filters = [
    {
        "field": "frdate",
        "type": "range",
        "operator": "eq",
        "start": "2024-12-01",
        "end": "2024-12-05",
        "relation": "and"
    },
    {
        "field": "todate",
        "type": "range",
        "operator": "eq",
        "start": "2024-12-01",
        "end": "2024-12-05",
        "relation": "and"
    }
]
secondary_filters = [
    {
        "field": "Status",
        "type": "single",
        "operator": "eq",
        "value": "Active",
        "relation": "and"
    }
]

# Step 1: Generate Prepared Requests
prepared_requests = create_request_urls(base_url, primary_filters, secondary_filters, headers=headers)

# Step 2: Generate Partition Paths for Primary Filters
for req in prepared_requests:
    partition_path = generate_pyspark_partition_path(base_path, req.url, primary_filters)
    print("Request URL:", req.url)
    print("Partition Path:", partition_path)
    print("---")




from urllib.parse import urlparse, parse_qs, quote, unquote

import os
import itertools
import json
from datetime import datetime as dt, timedelta
import requests
from itertools import product

def generate_date_range(start, end):
    """Generate a list of dates between start and end (inclusive)."""
    start_date = dt.strptime(start, "%Y-%m-%d")
    end_date = dt.strptime(end, "%Y-%m-%d")
    if start_date>end_date:
        raise Exception(f"start date {start_date} is greater than {end_date}")
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime("%Y-%m-%d")
        current_date += timedelta(days=1)

def build_primary_combinations(primary_filters):
    """Generate all combinations of primary filters."""
    filter_values = []
    for primary_filter in primary_filters:
        if primary_filter["type"] == "daterange":
            # Generate individual values for range
            if 'field' in primary_filter:
                field = primary_filter["field"]
                operator = primary_filter["operator"]
                values = [f"{field} {operator} '{date}'" for date in generate_date_range(primary_filter["start"], primary_filter["end"])]
            elif 'fields' in primary_filter:
                values = []
                operator = primary_filter["operator"]
                for date in generate_date_range(primary_filter["start"], primary_filter["end"]):
                    string = ''
                    for field in primary_filter['fields']:
                        string = string + f"{field} {operator} '{date}' "
                    values.append(string.strip(' '))
                # [f"{field} {operator} '{date}'" for date in generate_date_range(primary_filter["start"], primary_filter["end"]) for field in primary_filter['fields']]
        
        elif primary_filter["type"] == "list":
            field = primary_filter["field"]
            operator = primary_filter["operator"]
            # Generate individual values for list
            values = [f"{field} {operator} '{value}'" for value in primary_filter["values"]]
        
        else:
            raise ValueError(f"Unsupported filter type: {primary_filter['type']}")

        filter_values.append(values)

    # Generate all combinations of primary filters
    return list(product(*filter_values))


def build_secondary_clause(secondary_filters):
    """Combine all secondary filters into a single clause."""
    clauses = []
    for secondary_filter in secondary_filters:
        field = secondary_filter["field"]
        operator = secondary_filter["operator"]
        value = secondary_filter["value"]
        clause = f"{field} {operator} '{value}'"
        clauses.append(clause)
    return " and ".join(clauses)

def build_prepared_requests(base_url, config, auth=None, headers=None):
    """Generate all prepared requests based on primary and secondary filters."""
    prepared_requests = []
    primary_filters = config.get("primary_filters", [])
    secondary_filters = config.get("secondary_filters", [])

    # Handle empty filters
    if not primary_filters and not secondary_filters:
        # No filters, return base request
        req = requests.Request("GET", base_url, headers=headers, auth=auth)
        prepared_requests.append(req.prepare())
        return prepared_requests

    # Generate combinations of primary filters
    primary_combinations = build_primary_combinations(primary_filters) if primary_filters else [[]]

    # Build the secondary filter clause
    secondary_clause = build_secondary_clause(secondary_filters) if secondary_filters else ""

    # Combine primary combinations with secondary filters
    for primary_combination in primary_combinations:
        if primary_combination:
            primary_clause = " and ".join(primary_combination)
        else:
            primary_clause = ""

        if primary_clause and secondary_clause:
            full_query = f"{primary_clause} and {secondary_clause}"
        elif primary_clause:
            full_query = primary_clause
        elif secondary_clause:
            full_query = secondary_clause
        else:
            full_query = ""  # No filters at all

        if full_query:
            query_url = f"{base_url}?$filter={quote(full_query)}"
        else:
            query_url = base_url

        # Create the request
        req = requests.Request("GET", query_url, headers=headers, auth=auth)
        prepared_requests.append(req.prepare())

    return prepared_requests

def generate_pyspark_partition_path(base_path, request_url, filter_config):
    """
    Generate a PySpark-compatible partition path using primary filters 
    from a single request URL.

    Args:
        base_path (str): Base path or prefix for the partition path.
        request_url (str): The request URL containing query parameters.
        filter_config (dict): The filter configuration dictionary.

    Returns:
        str: A partition path based on filters.
    """
    # Parse the URL and extract query parameters
    parsed_url = urlparse(request_url)
    query_params = parse_qs(parsed_url.query)

    # Extract the $filter query string and decode it
    filter_string = query_params.get('$filter', [None])[0]
    if not filter_string:
        return ""  # Return empty if no filter string found

    filter_string = unquote(filter_string)  # Decode percent-encoded filter string

    # Dictionary to store key-value pairs for the partition path
    partition_data = {}

    # Process primary filters
    for filter_config_item in filter_config['primary_filters']:
        if "fields" in filter_config_item:  # Handling 'fields' case (e.g., fromDate and toDate)
            combined_field = "_".join([field.lower() for field in filter_config_item["fields"]])
            value = filter_config_item["start"]
            partition_data[combined_field] = value
        elif "field" in filter_config_item:  # Handling single 'field' case
            field = filter_config_item["field"]
            field_lower = field.lower()
            # Search for 'field eq' in the filter string
            for condition in filter_string.split("and"):
                condition = condition.strip()
                if condition.startswith(f"{field} eq"):
                    value = condition.split("eq")[1].strip(" '").lower()
                    partition_data[field_lower] = value

    # Construct partition segments
    partition_segments = [f"{key}={value}" for key, value in partition_data.items()]
    
    # Combine base path and segments to form the partition path
    partition_path = base_path + '/' + '/'.join(partition_segments)
    return partition_path

FILTER_CONFIG = '''{
    "primary_filters": [
        {
            "fields": ["todate", "fromdate"],
            "type": "daterange",
            "operator": "eq",
            "start": "2024-12-01",
            "end": "2024-12-05",
            "relation": "and"
        },
        {
            "field": "City",
            "type": "list",
            "operator": "eq",
            "values": [
                "Kolkata",
                "Delhi",
                "Mumbai"
            ],
            "relation": "or"
        }
    ],
    "secondary_filters": [
        {
            "field": "Status",
            "type": "single",
            "operator": "eq",
            "value": "Active",
            "relation": "and"
        }
    ]
}'''

filter_config = json.loads(FILTER_CONFIG)

base_url = "https://example.com/odata"
auth = ("username", "password")  # Example Basic Auth
headers = {"Accept": "application/json"}  # Example Headers

prepared_requests = build_prepared_requests(base_url, filter_config, auth=auth, headers=headers)

# Base path for PySpark partition paths
base_path = "s3://data-lake"

base_url = "https://example.com/odata"
auth = ("username", "password")  # Example Basic Auth
headers = {"Accept": "application/json"}  # Example Headers

prepared_requests = build_prepared_requests(base_url, filter_config, auth=auth, headers=headers)

# Base path for PySpark partition paths
base_path = "s3://data-lake"

# Generate PySpark-compatible partition paths
for prepared_request in prepared_requests:
    partition_path = generate_pyspark_partition_path(base_path, prepared_request.url, filter_config)
    print (prepared_request.url + '\n' + partition_path)


import requests
import os
import json
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to send an HTTP GET request
def send_request(url, auth=None, headers=None):
    try:
        response = requests.get(url, auth=auth, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Request failed for URL: {url}. Error: {e}")
        return None

# Function to validate the total count of data using $count
def validate_data_count(base_url, auth=None, headers=None, page_size=100):
    total_count = 0
    parsed_url = urlparse(base_url)

    try:
        # First, try to directly get the full count
        count_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}/$count"
        print(f"Validating count using URL: {count_url}")
        response = requests.get(count_url, auth=auth, headers=headers, timeout=10)
        response.raise_for_status()
        total_count = int(response.text)
        print(f"Total count validated: {total_count}")
    except requests.RequestException as e:
        print(f"Direct count failed. Paginating count... Error: {e}")
        # If direct count fails, paginate to get the total count
        for skip in range(0, 10**6, page_size):  # Arbitrarily large range
            query_params = parse_qs(parsed_url.query)
            query_params['$top'] = [page_size]
            query_params['$skip'] = [skip]

            paginated_query = urlencode(query_params, doseq=True)
            paginated_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                paginated_query,
                parsed_url.fragment
            ))

            print(f"Requesting paginated count at: {paginated_url}")
            response_data = send_request(paginated_url, auth, headers)
            if response_data and response_data.get('value'):
                total_count += len(response_data['value'])
            else:
                print("No more data during count pagination. Stopping count validation.")
                break

        print(f"Total count (paginated) validated: {total_count}")

    return total_count

# Function to paginate requests
def paginate_request(base_url, auth=None, headers=None, page_size=100, max_pages=10):
    combined_data = []
    parsed_url = urlparse(base_url)

    for page_number in range(1, max_pages + 1):
        query_params = parse_qs(parsed_url.query)
        query_params['$top'] = [page_size]
        query_params['$skip'] = [(page_number - 1) * page_size]

        paginated_query = urlencode(query_params, doseq=True)
        paginated_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            paginated_query,
            parsed_url.fragment
        ))

        print(f"Requesting page {page_number}: {paginated_url}")
        response_data = send_request(paginated_url, auth, headers)
        if response_data and response_data.get('value'):
            combined_data.extend(response_data['value'])
        else:
            print(f"No data received on page {page_number}. Stopping pagination.")
            break

    return combined_data

# Function to store data to a specific path
def store_data_to_path(data, directory_path, file_name):
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

        file_path = os.path.join(directory_path, file_name)
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Data successfully stored at: {file_path}")
    except Exception as e:
        print(f"Failed to store data. Error: {e}")

# Function to process multiple URLs concurrently
def process_multiple_urls_concurrently(url_list, auth=None, headers=None, page_size=100, max_pages=10, max_workers=1, output_dir="output"):
    all_data = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(paginate_request, url, auth, headers, page_size, max_pages): url
            for url in url_list
        }

        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                all_data[url] = result
                print(f"Completed processing for URL: {url}")
                
                # Validate count for this URL
                validated_count = validate_data_count(url, auth, headers, page_size)
                print(f"Validated count for {url}: {validated_count}")

                # Store the data into the output directory
                file_name = f"data_{url.split('/')[-1].split('?')[0]}.json"
                store_data_to_path(result, output_dir, file_name)

            except Exception as e:
                print(f"Error processing URL {url}: {e}")
                all_data[url] = None

    return all_data


url_list = [
    "https://example.com/odata/Entities",
    "https://example.com/odata/Items",
]

auth = ("username", "password")
headers = {"Accept": "application/json"}

all_data = process_multiple_urls_concurrently(
    url_list, auth=auth, headers=headers, page_size=50, max_pages=5, max_workers=1, output_dir="data_output"
)


# Function to validate the total count of data using paginated requests
def validate_data_count(base_url, auth=None, headers=None, page_size=100):
    total_count = 0
    parsed_url = urlparse(base_url)

    # Always paginate through the data and sum the counts
    for skip in range(0, 10**6, page_size):  # Arbitrarily large range
        query_params = parse_qs(parsed_url.query)
        query_params['$top'] = [page_size]
        query_params['$skip'] = [skip]

        paginated_query = urlencode(query_params, doseq=True)
        paginated_url = urlunparse((
            parsed_url.scheme,
            parsed_url.netloc,
            parsed_url.path,
            parsed_url.params,
            paginated_query,
            parsed_url.fragment
        ))

        print(f"Requesting paginated count at: {paginated_url}")
        response_data = send_request(paginated_url, auth, headers)
        if response_data and response_data.get('value'):
            total_count += len(response_data['value'])
        else:
            print("No more data during count pagination. Stopping count validation.")
            break

    print(f"Total count validated (paginated): {total_count}")
    return total_count


import time
import requests
from requests.exceptions import RequestException

def send_request(url, auth=None, headers=None, max_retries=3, initial_delay=1):
    """
    Send an HTTP GET request to the given URL with retry mechanism.
    
    Args:
        url (str): The request URL.
        auth (tuple): Authentication credentials (optional).
        headers (dict): Request headers (optional).
        max_retries (int): Maximum number of retry attempts (default=3).
        initial_delay (int): Initial retry delay in seconds (default=1).
    
    Returns:
        dict: The JSON response data or None on failure.
    """
    attempt = 0
    delay = initial_delay
    
    while attempt < max_retries:
        try:
            print(f"Attempt {attempt + 1}: Sending request to {url}")
            response = requests.get(url, auth=auth, headers=headers, timeout=30)
            
            # Check for successful response
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Request failed with status code {response.status_code}. Retrying...")
        
        except RequestException as e:
            print(f"Error during request: {e}. Retrying...")
        
        # Increment retry count and delay
        attempt += 1
        time.sleep(delay)
        delay *= 2  # Exponential backoff
    
    print(f"Request failed after {max_retries} attempts. Returning None.")
    return None

import time
import requests
from requests.exceptions import RequestException

def send_request(prepared_request, max_retries=3, initial_delay=1):
    """
    Send an HTTP request using a prepared request object with retry mechanism.

    Args:
        prepared_request (requests.PreparedRequest): The prepared request object.
        max_retries (int): Maximum number of retry attempts (default=3).
        initial_delay (int): Initial retry delay in seconds (default=1).

    Returns:
        dict: The JSON response data or None on failure.
    """
    session = requests.Session()
    attempt = 0
    delay = initial_delay

    while attempt < max_retries:
        try:
            print(f"Attempt {attempt + 1}: Sending request to {prepared_request.url}")
            response = session.send(prepared_request, timeout=30)

            # Check for successful response
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Request failed with status code {response.status_code}. Retrying...")

        except RequestException as e:
            print(f"Error during request: {e}. Retrying...")

        # Increment retry count and delay
        attempt += 1
        time.sleep(delay)
        delay *= 2  # Exponential backoff

    print(f"Request failed after {max_retries} attempts. Returning None.")
    return None


def paginate_request(prepared_request, send_request_fn, page_size=100):
    """
    Paginate requests and retrieve all data.

    Args:
        prepared_request (requests.PreparedRequest): The base prepared request object.
        send_request_fn (function): Function to send the request and retrieve a response.
        page_size (int): The number of items per page.

    Returns:
        list: A list containing all paginated response data.
    """
    session = requests.Session()
    all_data = []
    next_url = prepared_request.url  # Start with the base URL

    while next_url:
        print(f"Fetching data from: {next_url}")

        # Update the request with the current URL for pagination
        request = requests.Request(
            method=prepared_request.method,
            url=next_url,
            headers=prepared_request.headers,
            auth=prepared_request.auth,
        )
        prepared_paginated_request = session.prepare_request(request)

        # Send the request and retrieve data
        response_data = send_request_fn(prepared_paginated_request)
        if not response_data or "value" not in response_data:
            print("No more data to retrieve or invalid response.")
            break

        # Append the current batch of data
        all_data.extend(response_data["value"])

        # Check for pagination link
        next_url = response_data.get("@odata.nextLink", None)

    return all_data


def validate_data_count(prepared_request, send_request_fn, page_size=1000):
    """
    Validate data count by paginating through the count endpoint.

    Args:
        prepared_request (requests.PreparedRequest): The prepared request object.
        send_request_fn (function): Function to send the request and retrieve a response.
        page_size (int): The page size for count validation.

    Returns:
        int: The total validated count.
    """
    session = requests.Session()
    total_count = 0
    next_url = f"{prepared_request.url}/$count"

    while next_url:
        print(f"Validating count from: {next_url}")

        # Prepare the paginated count request
        request = requests.Request(
            method=prepared_request.method,
            url=next_url,
            headers=prepared_request.headers,
            auth=prepared_request.auth,
        )
        prepared_count_request = session.prepare_request(request)

        # Send the request and retrieve the count
        response_data = send_request_fn(prepared_count_request)
        if response_data is None:
            print("Failed to retrieve count data.")
            break

        # Increment total count
        total_count += int(response_data)

        # Check for pagination link in the count response
        next_url = response_data.get("@odata.nextLink", None)

    print(f"Total validated count: {total_count}")
    return total_count



1. CDS Views Availability & Exposure
Q1: Are CDS views already available and exposed via OData or another interface?
☐ Yes
☐ No, need to be created
☐ Not sure
🔎 Follow-up:
If Yes → Proceed with integration plan.
If No → Discuss CDS view creation effort with SAP functional team.
If Not sure → Involve SAP Basis or functional consultant to assess.

2. Data Sync Frequency
Q2: What is the expected frequency of data sync?
☐ Real-time
☐ Near real-time
☐ Batch
🔎 Follow-up:
Real-time → Favor DMS/CDC or SLT replication.
Batch → Glue or OData-based extractions may suffice.

3. Access to SLT or Replication Mechanism
Q3: Is SAP SLT or other replication tool available for CDC?
☐ SLT
☐ No
☐ 3rd party tools
☐ Not sure
🔎 Follow-up:
If SLT is available → Explore CDC replication.
If not → Batch/API-based strategies must be designed.

4. Volume of Daily Data Loads
Q4: What is the approximate volume of daily data?
☐ <100k
☐ 100k–1M
☐ >1M
☐ Not sure
🔎 Follow-up:
Large volumes → Use scalable ingestion (Glue, Spark, DMS full load).
Unclear volumes → Request volume samples or size reports.

5. Partitioning and Key Columns
Q5: Are there key columns for incremental loads?
☐ Yes
☐ Logic needed
☐ No
☐ Not sure
🔎 Follow-up:
If available → Optimize loads by filtering.
If not → Prepare for full table scans or artificial watermarking.

6. Primary Keys and Change Tracking
Q6: Are PKs and update timestamps present?
☐ Both
☐ Only PK
☐ Only timestamp
☐ Neither
☐ Not sure
🔎 Follow-up:
If both present → Enables efficient CDC or UPSERT logic.
If missing → Plan for full overwrite or log-based ingestion.

7. Sample Data for Testing
Q7: Can sample data (~1 day) be provided?
☐ Yes (dump or access)
☐ No
☐ Not sure
🔎 Follow-up:
If Yes → Begin integration POC.
If No → Request lower envs or sanitized data for dry runs.

8. Deletes and Soft Deletes
Q8: How are deletes handled?
☐ Soft
☐ Hard
☐ Both
☐ Not sure
🔎 Follow-up:
Soft deletes → Add filters in logic.
Hard deletes → Require CDC or delta log reconciliation.

9. Transactional vs Master Data Frequency
Q9: How frequently does each data type change?
☐ Transactional more
☐ Both frequently
☐ Rarely
☐ Not sure
🔎 Follow-up:
Frequent changes → Plan incremental load & schedule.
Rare changes → Less frequent master pulls acceptable.

10. Existing Staging Layer
Q10: Is there a staging layer like BW/DataSphere?
☐ Yes
☐ No
☐ Needs setup
☐ Not sure
🔎 Follow-up:
Use staging layer if available → Avoid direct OLTP load.
If not → Assess impact of direct load and data volume.

11. Integration Preference
Q11: Preferred data integration strategy?
☐ DMS
☐ Glue + PySpark
☐ OData API
☐ 3rd-party
☐ Open to suggestions
🔎 Follow-up:
Align integration choice with sync frequency, latency, volume, team skills.

12. Business Use-Cases & Querying
Q12: What will data be used for post-integration?
☐ Reporting
☐ Analytics
☐ Data lake
☐ Other
🔎 Follow-up:
Reporting → Schema modeling is critical.
Analytics → May require data denormalization.
Data lake → Format & partitioning matter.

13. Security & Access
Q13: Are there restrictions on data access?
☐ Yes
☐ No
☐ Not sure
🔎 Follow-up:
If Yes → Check compliance & set up secure credentials/roles.
If Not sure → Consult SAP Security team.

14. Environment Access (Dev/Test/Prod)
Q14: What environments are available?
☐ Dev/Test
☐ Test/Prod
☐ Prod only
☐ Not sure
🔎 Follow-up:
Dev/Test access → Start POC safely.
Prod only → Raise risk flag, consider non-prod mirror.

15. Expected Replication Lag
Q15: What replication lag is acceptable?
☐ <5 min
☐ <1 hour
☐ Daily
☐ Best effort
☐ Not sure
🔎 Follow-up:
Strict lag → DMS/SLT likely needed.
Relaxed lag → Batch or OData feasible.

16. Data Sensitivity & Masking
Q16: Are PII/sensitive fields masked or encrypted?
☐ Mask before ingest
☐ Mask after ingest
☐ No
☐ Not sure
🔎 Follow-up:
Masking needed → Add masking/encryption in ETL.
Check compliance & audit obligations.

17. Network & Connectivity Constraints
Q17: Are there connectivity restrictions?
☐ VPN/DirectConnect
☐ Firewall setup needed
☐ Public APIs only
☐ Not sure
🔎 Follow-up:
No connection yet → Plan VPC, routing, security group setup.

18. Audit Logging & Data Lineage
Q18: Is audit or lineage tracking required?
☐ Yes
☐ Partial
☐ No
☐ Not sure
🔎 Follow-up:
Yes → Build metadata lineage and job tracking logs.
No → Simplifies design, but verify future reporting needs.

19. Error Handling & Retry
Q19: Expected behavior on failures?
☐ Retry from checkpoint
☐ Skip & log
☐ Fail and alert
☐ Not sure
🔎 Follow-up:
Retry logic → Add checkpoints or idempotent loaders.
Fail-fast → Add alerting mechanism.

20. Data Model Alignment & Business Validation
Q20: Will business validate the integrated data model?
☐ Yes
☐ Tech team only
☐ No validation
☐ Not sure
🔎 Follow-up:
Business involvement → Set up UAT or test sign-off cycle.

Q1: What modules/data domains are available in Minda Sparsh for integration?
☐ Sales
☐ Procurement
☐ Inventory
☐ Customer-specific data
☐ Engineering/product config
☐ Others: ___________

🔎 Follow-up:
Helps determine data domain scope from Minda Sparsh. Clarifies functional reach of Sparsh vs SAP.

Q2: How is data stored and exposed from Minda Sparsh?
☐ Database tables (RDBMS)
☐ Flat files (CSV/Excel exports)
☐ REST/SOAP APIs
☐ Manual file uploads
☐ Not sure

🔎 Follow-up:
Determines integration strategy (e.g., JDBC pull, Snowpipe, API crawler, manual drops).

Q3: Is there a schema/data dictionary available for Minda Sparsh?
☐ Yes, complete schema
☐ Partial schema or data model
☐ No schema available
☐ Not sure

🔎 Follow-up:
If unavailable → Plan a schema discovery phase with the source team.

Q4: What are the frequency and modes of data availability from Minda Sparsh?
☐ Real-time via APIs
☐ Near real-time sync (every 15 min–hourly)
☐ Daily batch files
☐ Weekly/monthly drops
☐ Ad hoc/manual on request

🔎 Follow-up:
Aligns ingestion frequency in the lakehouse. If batch → Plan for Snowpipe or Glue scheduled jobs.

Q5: What fields or keys are available to join Minda Sparsh data with SAP or other systems?
☐ Model
☐ Variant
☐ Customer ID
☐ Part Number
☐ KIT ID
☐ No common fields
☐ Not sure

🔎 Follow-up:
Helps design master data harmonization. Missing keys → Requires mapping layer or enrichment logic.

Q6: What is the volume of data coming from Minda Sparsh per day/week?
☐ < 50k records
☐ 50k–500k records
☐ > 500k records
☐ Not sure

🔎 Follow-up:
Informs infrastructure decisions (compute, scaling). High volume → Prepare for stream/batch partitioning.

Q7: How is data versioning or change tracking handled in Sparsh?
☐ Time-stamped updates
☐ Change flags (insert/update/delete)
☐ Overwrites entire table/file
☐ No versioning (static snapshots)
☐ Not sure

🔎 Follow-up:
Critical for incremental loads and CDC logic. No versioning → Consider full loads with delta logic in lake.

Q8: Is historical data available in Minda Sparsh?
☐ Yes, full historical load
☐ Partial history (e.g., 3–6 months)
☐ Only current snapshot
☐ Not sure

🔎 Follow-up:
Impacts initial load strategy. Full history → Great for backtesting and modeling.

Q9: Are there any access/authentication requirements for Minda Sparsh?
☐ VPN access required
☐ Role-based credentials (DB/API)
☐ Public/internal endpoint available
☐ Access not yet configured
☐ Not sure

🔎 Follow-up:
Plan network access or authentication automation for scheduled jobs.

Q10: Are there known data quality issues or manual data manipulation in Minda Sparsh?
☐ Yes, frequent cleansing required
☐ Some transformation needed
☐ Data is clean and standardized
☐ Not sure

🔎 Follow-up:
Helps assess need for DQ pipelines and cleansing logic in ingestion/curation layers.

🔍 Section A – Source System Behavior & Access
Q1.1: What is the underlying technology stack of Minda Sparsh?
☐ SQL Server
☐ SAP HANA
☐ In-house custom DB
☐ Other: __________
📌 Follow-up: Helps determine how well it integrates with DMS, Glue, or needs custom ingestion.

Q1.2: Is Minda Sparsh hosted on-prem, in the cloud, or hybrid setup?
☐ On-prem
☐ Private cloud
☐ Public cloud (e.g., AWS, Azure)
☐ Hybrid
📌 Follow-up: This decides whether VPN, Direct Connect, or on-prem agent is needed.

Q1.3: Is the system OLTP or OLAP in nature?
☐ OLTP
☐ OLAP
☐ Mixed workload
📌 Follow-up: OLTP systems require more care in CDC and performance.

Q1.4: Does the system support exposing data via REST APIs, OData, or other services?
☐ Yes, APIs are available
☐ No, only DB/table level access
☐ Partially (custom APIs for specific modules)
📌 Follow-up: If APIs are available, consider API-based ingestion for certain modules.

📊 Section B – Volume & Change Rate (Minda Sparsh Specific)
Q2.1: What is the average and peak volume of data in key transactional modules (e.g., LTP, Indents, GC metrics)?
☐ <100k records/month
☐ 100k–1M records/month
☐ >1M records/month
📌 Follow-up: Required to size Glue jobs or Snowpipe streams.

Q2.2: Is the data in Minda Sparsh event-driven or batch-uploaded from other systems (e.g., SAP)?
☐ Real-time user entry
☐ Batch interface from SAP or Excel
☐ Mixed
📌 Follow-up: Impacts latency of sync and replication logic.

Q2.3: Do tables contain audit columns like created_at, updated_at, deleted_flag, etc.?
☐ Yes
☐ Partially
☐ No
📌 Follow-up: Essential for custom CDC via Glue.

Q2.4: Are historical versions of records maintained in the same table (Type 2 SCD) or overwritten?
☐ Maintained (versioned)
☐ Overwritten
☐ Depends on table
📌 Follow-up: If overwritten, need CDC or snapshot reconciliation logic.

🛠️ Section C – Integration Feasibility & Constraints
Q3.1: Is there a current interface exporting Minda Sparsh data to external systems?
☐ Yes (to SAP, BW, Excel)
☐ No
☐ Under evaluation
📌 Follow-up: Reuse possible or need to create new pipelines.

Q3.2: Who owns the schema definitions and can approve data extraction logic?
☐ Internal IT
☐ Functional team
☐ Third-party vendor
📌 Follow-up: Important for field mapping and FSD approvals.

Q3.3: Can we run lightweight discovery scripts on the DB (e.g., to assess data profile, table relationships)?
☐ Yes
☐ No
☐ Under Approval
📌 Follow-up: Helps design Glue jobs or data contracts.

🔒 Section D – Access, Authentication, and Network Setup
Q4.1: What is the authentication method supported for data extraction?
☐ SQL Auth / DB user
☐ SSO / OAuth
☐ Key-based API token
📌 Follow-up: Helps finalize connector configuration for Glue or DMS.

Q4.2: Are there specific IP allowlists or firewall rules required to access the system from AWS?
☐ Yes, need to open firewall
☐ Already configured
☐ Requires approval from network team
📌 Follow-up: Determines timeline and complexity for network setup.

Q4.3: Does the Minda Sparsh system undergo regular schema changes or column additions?
☐ Frequently
☐ Rarely
☐ Never (stable schema)
📌 Follow-up: Affects robustness of integration and schema evolution tracking.

A. Functional Expectations
Q1.1: What is the expected schema and granularity at the curated layer?
☐ Record-level (transactional)
☐ Daily/monthly aggregates
☐ Model/variant-level KPIs
📌 Follow-up: Impacts transformations, joins, and aggregations.

Q1.2: What transformations/enrichments must occur before data lands in curated/Snowflake?
☐ Derived columns (e.g., profit %, margin %)
☐ Dimension joins (e.g., customer master, KIT hierarchy)
☐ Row-level filters or quality rules
📌 Follow-up: Define business rules in transformation layer.

Q1.3: Are there specific schema naming conventions or harmonization rules to follow?
☐ Yes, project-level naming standards exist
☐ No, follow source schema
☐ Will be defined during modeling
📌 Follow-up: Helps ensure consistent datasets across Snowflake & Tableau.

Q1.4: Is historical data required (snapshots) or only latest-state data?
☐ Snapshot every load (historical)
☐ Overwrite (latest state)
☐ Depends on table
📌 Follow-up: Impacts storage, versioning, and query logic.

Q1.5: What is the expected data freshness for curated/BI use?
☐ Daily by X AM
☐ Hourly refresh
☐ Real-time (streaming or <5 min delay)
📌 Follow-up: Helps choose Snowpipe vs batch Glue job vs streaming.

📌 B. Technical Format & Ingestion Style
Q1.6: What file formats are preferred in structured/curated layers?
☐ Parquet
☐ CSV
☐ JSON
☐ Delta/Iceberg
📌 Follow-up: Affects storage efficiency and query performance.

Q1.7: Should the curated data be partitioned?
☐ Yes (e.g., by date/model/customer)
☐ No partitioning needed
📌 Follow-up: Enables faster queries and cost-optimized scans.

Q1.8: What is the preferred ingestion mechanism into Snowflake?
☐ Snowpipe (push from S3)
☐ Scheduled pull (external stage)
☐ Manual load or third-party tool
📌 Follow-up: Aligns with automation and access model.

Q1.9: How should we signal data readiness?
☐ File/folder naming pattern
☐ Marker file (e.g., _SUCCESS)
☐ Glue catalog/table update
☐ Email/notification
📌 Follow-up: Needed for orchestration and alerting setup.

📌 C. Validation, Alerts & SLAs
Q1.10: Will downstream teams validate load using record counts or control files?
☐ Yes, control totals or hash checksums expected
☐ No, only failure alerts
☐ Partial validation (row counts, null checks)
📌 Follow-up: Determines pre-curated validation strategy.

Q1.11: Are there DQ (Data Quality) rules to enforce before promoting data?
☐ Yes, business validation rules must pass
☐ No strict rules; pass-through
☐ In progress (will be defined)
📌 Follow-up: DQ rules can be centralized or per dataset.

Q1.12: How should we notify stakeholders about data load status?
☐ SNS/Email
☐ Slack/MS Teams alert
☐ CloudWatch alarm
☐ Logging only
📌 Follow-up: Required for operational transparency.

✅ Part 2: Internal Design Questions (Raw / Structured Layer Planning)
Use these during technical design, especially in data lake and S3 layer planning sessions.

📦 A. Raw Zone Planning (s3://lake/raw/...)
Q2.1: What folder structure will I use in the raw zone?
☐ <source>/<table>/<YYYY>/<MM>/<DD>/...
☐ Include timestamp folders or batch ID
📌 Follow-up: Drives consistency and future automation.

Q2.2: Should I store files as-is or convert to columnar formats?
☐ Store original (CSV/XML/JSON)
☐ Convert to Parquet during ingestion
📌 Follow-up: Converting early = faster downstream processing.

Q2.3: Should raw data be immutable (append-only) or overwritten?
☐ Append-only (recommended for audit)
☐ Overwrite allowed for corrections
📌 Follow-up: Influences data retention and lineage tracking.

Q2.4: Do I need to capture file-level metadata?
☐ Yes, store original filename, load time, source
☐ No, record-level metadata is enough
📌 Follow-up: Needed for traceability and audits.

🧱 B. Structured Zone Planning
Q2.5: What normalization/cleanup must be done before structured?
☐ Trim whitespace, fix types, drop nulls
☐ Standardize enums/codes (e.g., KIT categories)
📌 Follow-up: Ensures clean joins, valid filters in BI layer.

Q2.6: Should structured layer include harmonized keys and dimensions?
☐ Yes, join with master/reference tables
☐ Not needed, only pass raw fields
📌 Follow-up: Required for consistent cross-system analysis.

Q2.7: Should structured layer be Parquet with partitioning?
☐ Yes
☐ No
📌 Follow-up: Optimize for Snowflake external table or Athena.

Q2.8: Should Glue Catalog be used for structured zone discovery?
☐ Yes, for Athena + DQ + BI exploration
☐ No
📌 Follow-up: Enables previewing and schema tracking.

🧪 C. Governance & Lineage
Q2.9: Where should I log schema mismatches or DQ failures?
☐ Central logging (e.g., CloudWatch/S3)
☐ DQ dashboard
☐ Not required initially
📌 Follow-up: Supports monitoring and compliance.

Q2.10: Should we version structured data?
☐ Yes, daily snapshot folders or Delta/Apache Iceberg
☐ No, latest state is enough
📌 Follow-up: Helps rollback, audit, time-travel queries.

Q2.11: How will we track lineage from raw → structured → curated?
☐ Metadata tagging
☐ DataHub/Amundsen/Collibra
☐ Manual documentation
📌 Follow-up: Important for trust and traceability.


✅ Data Integration Assessment Questionnaire
🚩 Section 1: Source System Overview
Q1: What underlying technology does Minda Sparsh use?
☐ SQL Server
☐ SAP HANA
☐ Custom DB (specify): __________
☐ Other: __________

Q2: Where is Minda Sparsh hosted?
☐ On-prem
☐ Azure / AWS / Other cloud
☐ Hybrid

Q3: What type of workload is it?
☐ OLTP
☐ OLAP
☐ Mixed

Q4: How is Sparsh data exposed?
☐ Direct DB tables
☐ REST APIs / OData
☐ CSV/Excel exports
☐ Manual uploads
☐ Not sure

Q5: Are SAP CDS views already exposed via OData/API?
☐ Yes
☐ No, require creation
☐ Not sure

📊 Section 2: Data Volume & Frequency
Q6: What's the expected sync frequency?
☐ Real-time (CDC/SLT)
☐ Near real-time (hourly)
☐ Batch (daily+)

Q7: Average daily data volume?
☐ <100k records
☐ 100k–1M
☐ >1M
☐ Not sure

Q8: Data type update frequency?
☐ Mostly transactional
☐ Both transactional/master data frequently
☐ Rarely changes (mostly master data)

Q9: Historical data availability in Sparsh?
☐ Full history
☐ Partial (3–6 months)
☐ Only latest snapshot
☐ Not sure

🔄 Section 3: Incremental Load & Change Tracking
Q10: Are key columns available for incremental loading?
☐ Yes
☐ Logic needs creation
☐ No / not sure

Q11: Are Primary Keys (PKs) and timestamps available?
☐ Both present
☐ Only PK
☐ Only timestamp
☐ Neither / unsure

Q12: How does Sparsh handle deletes?
☐ Soft delete
☐ Hard delete
☐ Both
☐ Not sure

Q13: How is data versioning/change tracking handled?
☐ Timestamp updates
☐ Change flags
☐ Overwrite files/tables
☐ Static snapshots / No tracking
☐ Not sure

🔑 Section 4: Data Integration Preferences
Q14: What's the preferred integration strategy?
☐ DMS (CDC/replication)
☐ Glue + PySpark (batch)
☐ AWS AppFlow (API integration)
☐ Open to recommendations

Q15: Is SAP SLT or another CDC tool available?
☐ SLT
☐ Third-party
☐ No CDC available
☐ Not sure

🎯 Section 5: Downstream Use & Expectations
Q16: Intended downstream data use?
☐ Reporting
☐ Analytics
☐ Data lake exploration
☐ Other (specify): __________

Q17: Required data freshness?
☐ Real-time / <5 min lag
☐ Hourly
☐ Daily
☐ Best-effort

Q18: Schema granularity for curated layer?
☐ Transactional/record-level
☐ Daily/monthly aggregates
☐ KPI-level aggregation

Q19: Required data transformations before curation?
☐ Calculated fields (profit/margin)
☐ Master data joins
☐ Row-level quality filters

🔐 Section 6: Security, Compliance & Governance
Q20: Any PII/sensitive fields requiring masking/encryption?
☐ Mask before ingest
☐ Mask after ingest
☐ None / unsure

Q21: Restrictions on data access/security requirements?
☐ Specific access rules
☐ No restrictions
☐ Not sure

Q22: Audit logging/data lineage requirements?
☐ Required
☐ Partial logging sufficient
☐ Not required now

🌐 Section 7: Network & Connectivity
Q23: Connectivity method to source systems?
☐ VPN/DirectConnect
☐ Firewall configuration
☐ Public APIs only
☐ Not sure

Q24: Authentication mechanisms supported?
☐ SQL user/password
☐ SSO/OAuth
☐ API key/token

Q25: Firewall/IP restrictions?
☐ Firewall changes required
☐ Already configured
☐ Pending approval

🛠️ Section 8: Data Quality & Validation
Q26: Known data quality issues/manual intervention?
☐ Frequent data cleansing needed
☐ Minor transformations required
☐ Data is clean and ready

Q27: Downstream validation requirements?
☐ Control totals/checksums required
☐ Row-count/basic validation
☐ No strict validation

📂 Section 9: Internal Data Lake Layer Design
Q28: Raw layer data storage preference?
☐ Original format (CSV/XML/JSON)
☐ Convert immediately to Parquet

Q29: Raw layer folder structure preference?
☐ <source>/<table>/<YYYY>/<MM>/<DD>
☐ Include batch ID or timestamps

Q30: Raw data immutability?
☐ Append-only
☐ Overwrite allowed

Q31: Structured layer transformations?
☐ Normalize/enrich fields
☐ Standardize codes/enums
☐ Minimal cleanup

Q32: Structured layer partitioning & format?
☐ Parquet + partitioning (by date/model)
☐ Simple structure (no partitioning)

Q33: Glue Catalog for structured data discovery?
☐ Required (Athena/BI)
☐ Not required

🧪 Section 10: Environment & Testing
Q34: Available environments for testing/deployment?
☐ Dev/Test
☐ Test/Prod
☐ Prod only

Q35: Can sample/test data (~1-day extract) be provided?
☐ Yes, immediately
☐ No, requires sanitized samples
☐ Not sure

Q36: Business validation of data models post-integration?
☐ Yes (business sign-off required)
☐ Tech team validation only
☐ No validation planned

Q37: Error handling & retry strategy on failures?
☐ Retry from checkpoint
☐ Skip and log errors
☐ Fail-fast and notify immediately

📌 Section 11: Schema Ownership & Evolution
Q38: Schema definitions & extraction approvals owned by?
☐ Internal IT
☐ Functional team
☐ Vendor/external

Q39: Schema change frequency in Sparsh?
☐ Frequent
☐ Rarely
☐ Never (stable schema)

Q40: Schema/data dictionary availability for Sparsh?
☐ Full schema available
☐ Partial schema
☐ No schema

1. Complexity of Infrastructure and Management
AWS DMS:
Requires additional overhead—replication instances, endpoints setup, and ongoing management. Monitoring CDC tasks, tuning replication instances, and handling DMS logs involve significant effort.

AWS Glue:
Fully managed ETL service. Minimal infrastructure management. Serverless execution reduces operational complexity significantly.

2. Limited Data Transformation Capabilities
AWS DMS:
Primarily designed for migration or CDC tasks. It provides minimal transformation capability (basic filtering or simple renaming). Complex transformations (aggregations, joins, enrichments) aren’t natively supported.

AWS Glue:
Offers powerful transformation via PySpark scripts. Complex cleansing, aggregation, schema evolution, and enrichment processes are easy to manage and scale.

3. Cost Implications
AWS DMS:
Charges based on replication instance hours and storage. Continuous replication with large datasets or multiple environments can quickly escalate costs, especially if always-on CDC is required.

AWS Glue:
Pay-as-you-go (serverless pricing). Charges only for the compute time (job execution). Cost-effective for batch or event-driven workloads.

4. Schema Evolution and Flexibility
AWS DMS:
Less flexible with frequent schema changes in source databases. Altered source schemas often require manual interventions—table reloads or reconfiguration of endpoints and tasks.

AWS Glue:
Easily handles schema changes. Glue crawlers can auto-discover schema evolution, making it easier to maintain and adapt ETL processes dynamically.

5. Integration Limitations
AWS DMS:
Primarily database-centric (RDBMS). Limited capabilities for non-relational, API-driven, or semi-structured data (e.g., JSON, XML, OData).

AWS Glue:
Highly flexible—supports relational (via JDBC), semi-structured data, JSON, XML, and REST APIs (via custom scripts). Ideal for broader integration scenarios.

6. CDC Complexity and Constraints
AWS DMS:
CDC requires source database configurations (binlogs, transaction logs) that often require elevated privileges or source DB reconfiguration. Many clients hesitate or refuse due to security and operational concerns.

AWS Glue:
Does not need CDC-specific setups. Instead, Glue ETL scripts leverage incremental loads via audit columns, timestamps, or API endpoints—no deep database configurations required.

7. Security and Compliance
AWS DMS:
Elevated DB access often required (e.g., sysadmin roles in SQL Server). Higher security scrutiny and potential audit challenges.

AWS Glue:
Limited permissions required (typically read-only on specific tables/views). Easier to comply with strict data governance and security policies.

8. Network and Resource Overhead
AWS DMS:
Requires persistent connections between replication instances and databases. This continuous network load must be managed, especially over VPN or TGW connections—introducing latency or connectivity issues.

AWS Glue:
Works effectively in batch modes or triggered jobs, optimizing network usage and allowing controlled data transfers.

🎯 When AWS DMS might still make sense:
Real-time CDC is absolutely mandatory.

Direct DB-to-DB migrations with minimal transformations.

However, given your scenario (SAP integration, Azure SQL, OData, complex transformations, schema evolution), AWS Glue typically proves superior in simplicity, flexibility, and total cost of ownership.

🚩 Recommended Response (If Client Challenges)
"AWS DMS is excellent for straightforward, database-level migration or pure CDC use cases. However, considering the complexities of transformations, schema evolution, source system constraints, and operational overhead of DMS instances, AWS Glue is more agile, cost-effective, scalable, and easier to manage for our specific use case involving SAP OData, Minda Sparsh JDBC connectivity, and complex data harmonization."


✅ Support Required at Source DB (for DMS)
🔐 Database Configuration by DBA
Enable CDC or equivalent logging:

For SQL Server: Must enable MS-CDC or transactional replication. Required on each table involved.

For Oracle: Enable supplemental logging, archiving, and grant logmining access if using LogMiner.

For PostgreSQL: Set wal_level = logical, configure replication slots and replication user roles.

Retain logs: Set up retention policies for transaction logs to avoid data loss between DMS captures.

Create replication user:

User must have SELECT, REPLICATION, and sometimes EXECUTE rights depending on the DB type.

Firewall/IP allowlisting: Ensure replication instance’s IPs are whitelisted at source network layer.

Schema stability: DMS prefers a stable schema during migration; frequent changes can lead to errors.

👨‍💻 Support Expected from Source DBA & Business
Team	Responsibility
DBA	Enable CDC, configure logs, grant roles, open firewall ports, monitor health
IT/Infra Team	Provide VPN connectivity, validate TLS/SSL certs if needed
Business/SMEs	Confirm table ownership, provide key business fields for CDC & validation logic
🛠️ Supported Source DB Versions (from DMS UG)​dms-ug.pdf#Welcome
Microsoft SQL Server: 2012 to 2022 (except Express edition which is not supported)

Oracle: 10.2 → 12.2, 18c, 19c (Enterprise/Standard)

MySQL: 5.5 → 8.0 (DMS 3.4+)

PostgreSQL: 9.6 → 16.x (DMS 3.5.3+ needed for latest)

MariaDB: 10.0.24 → 10.6 (MySQL-compatible)

IBM Db2 LUW: 9.7, 10.1, 10.5, 11.1, 11.5

SAP ASE: 12.5 → 16

MongoDB: 3.x → 6.0

📌 Always validate DMS version compatibility for source features like CDC, compression, and secure LOB handling.

✅ SQL Server – Source DB Prep Scripts for AWS DMS
🔹 1. Enable CDC at the Database Level
sql
Copy
Edit
USE [YourDatabaseName];
EXEC sys.sp_cdc_enable_db;
🔹 2. Enable CDC on Specific Tables
Repeat this for each table you want to replicate via DMS:

sql
Copy
Edit
EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name = N'YourTableName',
@role_name = NULL,
@supports_net_changes = 1;
You may customize @role_name for tighter access control.

Set @supports_net_changes = 1 if DMS is using net changes for replication.

🔹 3. Verify CDC is Enabled
Check database-level:

sql
Copy
Edit
SELECT name, is_cdc_enabled FROM sys.databases WHERE name = 'YourDatabaseName';
Check table-level:

sql
Copy
Edit
SELECT name, is_tracked_by_cdc FROM sys.tables WHERE name = 'YourTableName';
🔹 4. Grant Access to DMS Replication User
Create a new user or assign to existing login used by DMS:

sql
Copy
Edit
CREATE LOGIN dms_user WITH PASSWORD = 'StrongPassword!123';
CREATE USER dms_user FOR LOGIN dms_user;
ALTER ROLE db_owner ADD MEMBER dms_user;
📌 Least-privilege principle is recommended. At minimum, grant:

sql
Copy
Edit
GRANT SELECT ON SCHEMA :: dbo TO dms_user;
GRANT EXECUTE ON SCHEMA :: cdc TO dms_user;
🔹 5. Ensure Transaction Log Retention
CDC requires transaction logs to be retained long enough for DMS to capture changes.

Set recovery model to FULL and ensure regular log backups:

sql
Copy
Edit
ALTER DATABASE [YourDatabaseName] SET RECOVERY FULL;
-- Use a SQL Agent Job or scheduled backup solution
BACKUP LOG [YourDatabaseName] TO DISK = 'C:\Backups\YourDB_LogBackup.trn';
🔹 6. Firewall and Network Allowlisting
Ensure the replication instance IP from AWS (or NAT gateway) is whitelisted in the Azure SQL Server Network Rules if hosted in a VM, or via NSG and routing tables.

⚠️ Important Considerations
These scripts only apply if SQL Server is self-hosted in a VM (not Azure SQL DB PaaS, which doesn't support CDC for DMS).

If Minda Sparsh is using Azure SQL Managed Instance, CDC may be supported but needs version validation.

✅ Minda Sparsh – Data Ingestion Design Document
1. Overview
This design outlines the secure, orchestrated ingestion of data from two primary source systems—SAP S4HANA and the Azure-hosted Minda Sparsh database—into the AWS ecosystem and onward to Snowflake. It is designed for modularity, auditability, and future scalability.

2. Source Systems
a. SAP S4HANA
Interface: CDS Views exposed via OData API

Access Method: AWS AppFlow (OData connector)

Authentication: To be finalized (OAuth/API Key/Basic Auth)

Frequency: Daily batch

Incremental Logic: Based on last updated timestamps (to be confirmed with client)

b. Minda Sparsh (Azure-hosted SQL Server)
Interface: JDBC

Access Method: AWS Glue PySpark job with JDBC connector

Authentication: Stored in AWS Secrets Manager

Frequency: Daily batch

Incremental Logic: Timestamp-based filtering

3. Network & Account Architecture
Source Systems Hosted in Azure

SAP and Minda Sparsh database are hosted in Azure

Transit Gateway Account (AWS)

Site-to-site VPN and routing between Azure and AWS environments

Target AWS Accounts

Dev, QA, and Prod accounts (to be provisioned)

All services (Glue, AppFlow, Secrets Manager, S3, SNS) are deployed within these accounts

4. Ingestion & Orchestration Flow
SAP Flow (via AppFlow):
Glue Workflow orchestrates entire flow

Glue Shell Job triggers the AppFlow flow

AppFlow pulls data from SAP OData and lands it in S3 Raw Layer

Glue logs the run metadata into S3 log folder

SNS Notification is sent to Snowflake team (topic per environment)

Minda Sparsh Flow (via JDBC):
Glue Workflow triggers a Glue PySpark Job

Glue connects to Minda SQL Server via JDBC, ingests data to S3 Raw Layer

Optional masking/transformations applied in PySpark

Glue logs metadata to S3 log folder

SNS Notification is sent to Snowflake team

5. Target System
Snowflake Data Warehouse

Listens to SNS events via Snowpipe or EventBridge

Loads data from external stage in S3 into Raw Layer

Further modeling into Curated and Business layers (owned by Snowflake team)

6. Logging & Metadata Tracking
All ingestion runs log metadata (run ID, timestamps, row counts, status) into S3

Logs are partitioned by date and source

Monitoring via Athena is planned for querying log events

Notifications are sent through SNS for success/failure tracking

7. Security Considerations
Encryption at Rest: S3 Buckets use AWS KMS-managed keys (handled by DevOps)

Secrets Management: All DB/API credentials are stored in AWS Secrets Manager

Data Masking: Sensitive fields (as defined by BA team) are masked in Glue using PySpark logic

8. Version Control & Future Automation
Code (Glue scripts, flow definitions) is stored in AWS CodeCommit

No CICD in current phase; design allows for future integration with CodePipeline or GitHub Actions

✅ 9. Text-Based Architecture Flow Diagram
pgsql
Copy
Edit
        +-------------------+                       +-------------------------+
        |  SAP S4HANA       |                       |  Minda Sparsh SQL Server|
        |  (OData API)      |                       |  (Azure-hosted DB)      |
        +--------+----------+                       +-----------+-------------+
                 |                                              |
         CDS Views exposed                          JDBC Connection from Glue
                 |                                              |
         AWS AppFlow (OData Connector)                AWS Glue PySpark Job (JDBC)
                 |                                              |
                 v                                              v
        +-------------------+                       +-------------------------+
        |   S3 Raw Layer    |<----------------------+    Ingested Files       |
        +--------+----------+                       +-----------+-------------+
                 |                                              |
        Metadata logs + job run info                 Metadata logs + masking
                 |                                              |
         AWS Glue Workflow (Dev/QA/Prod)   <--------+  Orchestrates both jobs
                 |
         +----------------------------+
         |  SNS Notification (per env)|
         +-------------+--------------+
                       |
              Snowflake Raw Layer
             (via Snowpipe/EventBridge)




#########################################TASK00#########################################
#IMPORTS
print ("STARTING JOB")

print ("STARTED TASK00 IMPORTS")

import sys
import boto3
from botocore.exceptions import ClientError
import time
from datetime import datetime, timedelta  
import json
import queue 
from datetime import datetime as dt, timedelta  

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F, types as T

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

appflow_client = boto3.client('appflow')
s3_client = boto3.resource('s3')
glue_client = boto3.client("glue")

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ODATA_PATH', 'FILTER_CONFIG', 'SELECT_FIELDS','VALIDATION_CONFIG' ,'MAX_RETRIES', 'MAX_PARALLELISM', 'MAX_RUN', 'TARGET_BUCKET_INGEST', 'TARGET_BUCKET_STRUCTURED', 'TARGET_BUCKET_TEMP'])
# args = getResolvedOptions(sys.argv, ['JOB_NAME', 'FILTER_CONFIG', 'SELECT_FIELDS', 'VALIDATION_CONFIG', 'MAX_RETRIES', 'MAX_PARALLELISM', 'MAX_RUN', 'TARGET_BUCKET_INGEST', 'TARGET_BUCKET_STRUCTURED', 'TARGET_BUCKET_TEMP'])

if '--WORKFLOW_NAME' in sys.argv and '--WORKFLOW_RUN_ID' in sys.argv:
    glue_args = getResolvedOptions(
        sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID']
    )
    workflow_args = glue_client.get_workflow_run_properties(
        Name=glue_args['WORKFLOW_NAME'], 
        RunId=glue_args['WORKFLOW_RUN_ID']
    ).get('RunProperties', 'Not Found')
    print (f'running with workflow_args : {workflow_args}')
else:
    print (f'running without workflow_args')

job.init(args['JOB_NAME'], args)

print('##########TASK00-IMPORTS-COMPLETED-SUCCESSFULLY##########')
#########################################TASK01#########################################
#PARAMETERS
print ("STARTED TASK01 PARAMETERS INITIALIZING")

appflow_client = boto3.client('appflow')
s3_client = boto3.resource('s3')

JOB_NAME = str(args['JOB_NAME'])  
JOB_RUN_ID = dt.now().strftime('%Y%m%d%H%M%S') 
ODATA_PATH = str(args['ODATA_PATH']) 
FILTER_CONFIG = str(args['FILTER_CONFIG']) 
SELECT_FIELDS = str(args['SELECT_FIELDS'])
VALIDATION_CONFIG = str(args['VALIDATION_CONFIG']) 
MAX_RETRIES = str(args['MAX_RETRIES'])
MAX_PARALLELISM = int(args['MAX_PARALLELISM'])
MAX_RUN = int(args['MAX_RUN'])
TARGET_BUCKET_INGEST = str(args['TARGET_BUCKET_INGEST'])
TARGET_BUCKET_STRUCTURED = str(args['TARGET_BUCKET_STRUCTURED'])
TARGET_BUCKET_TEMP = str(args['TARGET_BUCKET_TEMP'])

glue_job = 'nbtest_prod_jkt_ztbsaletgtq02srv_ztbsaletgtq02results_sap_appflow_s3_ingesthistory'
app, env, client, domain, entity, source, connect, target, action = glue_job.split("_")

flow_name = f"appflow-{env}-{client}-{domain}-{entity}-{source}-{target}-{action}"
appflow_ingest_prefix = f'{source}/{action}'
# appflow_ingest_path = f's3://{TARGET_BUCKET_INGEST}/{appflow_ingest_prefix}'
appflow_structured_prefix = f'{source}/{domain}/{entity}'
# appflow_structured_path = f's3://{TARGET_BUCKET_STRUCTURED}/{appflow_structured_prefix}'

source_target_mapping_json_string = '''
{
    "A0CALMONTH"    : "month",
    "A0CALDAY"      : "date",
    "A0COMP_CODE"   : "comp_code",
    "ZC_TERR"       : "terr_code",
    "A0MATERIAL"    : "material_code",
    "A0MATERIAL_T"  : "material_description",
    "ZTGT_QTY"      : "tgt_quantity",
    "ZTGT_VAL"      : "tgt_value"
}
'''

print(f"JOB_NAME: {JOB_NAME}")
print(f"JOB_RUN_ID: {JOB_RUN_ID}")
print(f"ODATA_PATH: {ODATA_PATH}")  
print(f"FILTER_CONFIG: {FILTER_CONFIG}")  
print(f"SELECT_FIELDS: {SELECT_FIELDS}")
print(f"VALIDATION_CONFIG: {VALIDATION_CONFIG}") 
print(f"MAX_RETRIES: {MAX_RETRIES}")  
print(f"MAX_PARALLELISM: {MAX_PARALLELISM}")  
print(f"MAX_RUN: {MAX_RUN}")
print(f"TARGET_BUCKET_INGEST: {TARGET_BUCKET_INGEST}")  
print(f"TARGET_BUCKET_STRUCTURED: {TARGET_BUCKET_STRUCTURED}")  
print(f"TARGET_BUCKET_TEMP: {TARGET_BUCKET_TEMP}")

print(f"app: {app}")
print(f"env: {env}")
print(f"client: {client}")  
print(f"domain: {domain}")  
print(f"entity: {entity}")  
print(f"source: {source}")
print(f"connect: {connect}")
print(f"target: {target}")
print(f"action: {action}")

print(f"appflow: {flow_name}")
print(f"appflow_ingest_prefix: {appflow_ingest_prefix}")
print(f"appflow_structured_prefix: {appflow_structured_prefix}")

print(f"source_target_mapping_json_string: {source_target_mapping_json_string}")

try:
    validation_config = json.loads(VALIDATION_CONFIG)
    select_fields = json.loads(SELECT_FIELDS)['FIELDS']
except Exception as e:
    raise Exception (f'TASK01-PARAMETERS-INITIALIZATION FAILED WITH ERROR {e}')
    
try:
    filter_field = ''
    filter_values = []
    filter_type = ''
    filter_config = json.loads(FILTER_CONFIG)
    # validation_config = json.loads(VALIDATION_CONFIG)
    if filter_config.get('filter_field', '_') != '_':
        filter_field = filter_config['filter_field']
        print(f"filter_field: {filter_field}")
        if filter_config.get('filter_type', '_') not in ['str', 'daterange', 'list']:
            raise Exception ('Missing or Incorrect filter_type')
        else:
            if filter_config.get('filter_type', '_') == 'list':
                if filter_config.get('filter_values', '_') != '_':
                    filter_values = filter_config['filter_values']
                    filter_values = [filter_value.strip(' ') for filter_value in filter_values.split(',')]
                    print(f"filter_values: {filter_values}")
                else:
                    raise Exception ('Missing or Incorrect filter_type')
            elif filter_config.get('filter_type', '_') == 'daterange':
                if filter_config.get('filter_values', '_').get('start_date', '_') != '_':
                    start_date_str = filter_config['filter_values']['start_date']
                    end_date_str = filter_config['filter_values']['end_date']
                    start_date = dt.strptime(start_date_str, '%Y-%m-%d')  
                    end_date = dt.strptime(end_date_str, '%Y-%m-%d') 
                    # Check if start_date is greater than end_date  
                    if start_date > end_date:  
                        raise ValueError("Start date : {start_date} cannot be greater than end date {end_date}.")
                    # Generate the range of dates in 'YYYYMMDD' format  
                    filter_values = [(start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)]
                    print(f"filter_values: {filter_values}")
                else:
                    raise Exception ('Missing or Incorrect filter_type')
    else:
        print('No Filter Applied')
except Exception as e:
    raise Exception (f'TASK01-PARAMETERS-INITIALIZATION FAILED WITH ERROR {e}')

print('##########TASK01-PARAMETERS-INITIALIZED-COMPLETED-SUCCESSFULLY##########')
# raise Exception('fORCED')
############################################TASK03############################################
print ("STARTED TASK03 UDFs Defination")
class AppFlowWrapper:
    def __init__(self, flow_name, flow_client, flow_config, max_retries):
        """
        Initialize the AppFlowWrapper with a Boto3 AppFlow client supplied externally.

        :param flow_client: The AppFlow client object supplied during initialization.
        :param flow_name: The AppFlow name object supplied during initialization.
        """
        self.flow_name = flow_name
        self.client = flow_client
        self.flow_config = flow_config
        self.execution_id = None
        self.retry = 0
        self.max_retries = max_retries
        self.execution_completed = False
        self.started = False
        self.records_processed = 0
        self.dt = self.flow_config.get('tasks')[0].get('taskProperties', {}).get('VALUE', 'all_dates')

        
    def flow_exists(self):
        """
        Check if a flow with the given name exists.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        try:
            self.client.describe_flow(flowName=flow_name)
        except Exception as e:
            print (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            return True

    def start_flow(self):
        """
        Start an AppFlow flow execution.

        :param flow_name: The name of the flow to start.
        :param client_token: (Optional) A unique, case-sensitive string to ensure idempotency.
        :return: The execution ID of the started flow, or None in case of error.
        """
        try:
            flow_name = self.flow_name
            response = self.client.start_flow(
                flowName=flow_name
            )
            execution_id = response.get('executionId')
            dt = self.dt
            print(f"{flow_name} : {dt} : {execution_id} started scuccessfully")
            self.execution_id = execution_id
            self.execution_start_time = time.time()
            time.sleep(1)
        except Exception as e:
            print (f"{flow_name} Error in starting flow : {e}")
            return False
        else:
            return True
    
    def get_execution_status(self):
        """
        Get the status of a specific flow execution.

        :param flow_name: The name of the flow.
        :param execution_id: The execution ID to check.
        :return: A tuple containing the execution status and the execution result (if available).
        """
        flow_name = self.flow_name
        execution_status = False
        dt = self.dt

        if self.execution_id==None:
            raise Exception(f"{flow_name} : {dt} : not yet started")
        else:
            execution_id = self.execution_id
        try:
            response = appflow_client.describe_flow_execution_records(flowName=flow_name)
            flow_executions = response.get('flowExecutions')
            for flow_execution in flow_executions:
                if flow_execution.get('executionId')==execution_id:
                    execution_status = flow_execution.get('executionStatus')
                    self.execution_status = flow_execution.get('executionStatus')
                    self.records_processed = flow_execution.get('executionResult').get('recordsProcessed', 0)
            if (execution_status==False):
                while 'nextToken' in response:
                    next_token = response.get('nextToken')
                    response = appflow_client.describe_flow_execution_records(flowName=flow_name, nextToken = next_token)
                    flow_executions = response.get('flowExecutions')
                    for flow_execution in flow_executions:
                        if flow_execution.get('executionId')==execution_id:
                            execution_status = flow_execution.get('executionStatus')
                            self.execution_status = flow_execution.get('executionStatus')
                            self.records_processed = flow_execution.get('executionResult').get('recordsProcessed', 0)
                            break
        except Exception as e:
            print(f"{flow_name} : {execution_id} : {dt} --> {e}")
        finally:
            if (execution_status):
                if self.in_terminal_state():
                    self.execution_completed = True
                return execution_status
            else:
                print(f"{flow_name} : {execution_id} : {dt} not found")
                #raise Exception((f"Execution ID '{execution_id}' not found for flow '{flow_name}'."))
                execution_status = 'UnKnown'
                self.execution_status = execution_status
                self.records_processed = 0
                if self.in_terminal_state():
                    self.execution_completed = True
                return execution_status

    def create_flow(self, flow_config):
        """
        Create a new AppFlow with the specified configuration.
        :param flow_name: Name of the new AppFlow.
        :param flow_config: Dictionary containing the flow configuration.
        :return: Flow creation status.
        """
        flow_name = self.flow_name
        dt = self.dt
        try:
            response = self.client.create_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"{flow_name} : {dt} created successfully")
            print (response)
        except Exception as e:
            print (f"{flow_name} : {dt} Unexpected error while creating flow : {e}")
            return False
        else:
            return True
        
    def update_flow(self, flow_config):
        """
        Update an existing flow with the given configuration.

        :param flow_name: The name of the flow to update.
        :param flow_config: A dictionary containing the updated flow configuration.
        """
        flow_name = self.flow_name
        dt = self.dt
        try:
            response = self.client.update_flow(
                flowName=flow_name,
                **flow_config
            )
            print(f"{flow_name} : {dt} updated successfully.")
        except Exception as e:
            print (f"{flow_name} : {dt} Failed to update flow : {e}")
            return False
        else:
            return True
        
    def create_or_update_flow(self):
        """
        Create a new flow or update an existing flow.

        :param flow_name: The name of the flow to create or update.
        :param flow_config: A dictionary containing the flow configuration.
        :return: True if the flow was created or updated successfully, False otherwise.
        """
        flow_config = self.flow_config
        flow_name = self.flow_name
        try:
            if self.flow_exists():
                print(f"{flow_name} : exists -> Updating...")
                self.update_flow(flow_config)
                #print(f"Flow '{flow_name}' updated successfully.")
            else:
                print(f"{flow_name} : does not exist -> Creating...")
                self.create_flow(flow_config)
                #print(f"Flow '{flow_name}' created successfully.")
        except Exception as e:
            print (f"Failed to create or update flow '{flow_name}': {e}")
            # return False
        else:
            return True
        
    def in_terminal_state(self):
        """
        Check if a flow execution has reached terminal state or not.

        :param flow_name: The name of the flow to check.
        :return: True if the flow exists, False otherwise.
        """
        flow_name = self.flow_name
        execution_id = self.execution_id
        dt = self.dt
        try:
            flow_status = self.execution_status
        except Exception as e:
            print (f"{flow_name} : {execution_id} : {dt} Error checking flow status exists: {e}")
            #raise (f"Error checking if flow '{flow_name}' exists: {e}")
            return False
        else:
            if flow_status in ['Successful', 'Error', 'CancelStarted', 'Canceled']:
                return True
            else:
                return False
    
    def create_and_start(self):
        if (self.execution_completed==False):
            self.create_or_update_flow()
            self.start_flow()
            self.get_execution_status()
            execution_id = self.execution_id
            self.retry = self.retry + 1
            self.started = True
            print (f'{self.flow_name} : {self.execution_id} : {self.dt} : retry -> {self.retry-1}')
        
    def excetion_monitor_and_retry(self):
        execution_id = self.execution_id
        self.get_execution_status()
        if self.execution_status=='Successful':
            return (True, self.execution_status)
        else:
            if self.retry<self.max_retries:
                if self.execution_status in ['Error', 'CancelStarted', 'Canceled']:
                    print (f"{flow_name} : {execution_id} : {dt} Restarting Flow")
                    self.create_and_start()
                else:
                    return (False, self.execution_status)
            else:
                return (True, self.execution_status)

def generate_date_range(start_date_str, end_date_str):  
    try:  
        # Parse the input date strings into datetime objects  
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')  
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')  
          
        # Check if start_date is greater than end_date  
        if start_date > end_date:  
            raise ValueError("Start date : {start_date} cannot be greater than end date {end_date}.")
          
        # Generate the range of dates in 'YYYYMMDD' format  
        date_range = [(start_date + timedelta(days=x)).strftime('%Y%m%d') for x in range((end_date - start_date).days + 1)]  
          
        return date_range
    except Exception as e:  
        raise Exception(f"An unexpected error occurred: {e}")
        
# print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, FILTER_FIELD, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
def push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, execution_id, dt, FILTER_FIELD, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED):
    source_read_path = f's3://{TARGET_BUCKET_INGEST}/{appflow_ingest_prefix}' + '/' + flow_name + '/' + execution_id
    target_write_path = f's3://{TARGET_BUCKET_STRUCTURED}/{appflow_structured_prefix}'
    df = spark.read.format('csv').options(header='true').load(source_read_path)\
            .withColumn('last_updated_datetime', F.current_timestamp().cast('string'))
    dt = dt
    if dt=='all_dates':
        mapped_filter_field = source_target_mapping_dict[FILTER_FIELD]
        df = df.withColumn(FILTER_FIELD.lower(), F.col(mapped_filter_field))
        df.repartition(1).write.mode('append').option("header", "true").format("csv").partitionBy(FILTER_FIELD.lower()).save(target_write_path)
    else:
        target_write_path = target_write_path + '/' + f'{FILTER_FIELD.lower()}={dt}'
        df.drop(FILTER_FIELD.lower()).repartition(1).write.mode('overwrite').option("header", "true").format("csv").save(target_write_path)
        
def create_queue_from_list(task_list):  
    task_queue = queue.Queue()  
    for task in task_list:  
        task_queue.put(task)  
    return task_queue

def monitor_tasks(task_queue, n):  
    active_tasks = []  
    passed_dts = []
    failed_dts = []
    while not task_queue.empty() or active_tasks:  
        # Start monitoring up to n tasks  
        while len(active_tasks) < n and not task_queue.empty():  
            task = task_queue.get()  
            active_tasks.append(task) 
            # print(f"Started monitoring task {task.dt}")  
  
        # Process active tasks
        try:
            for task in active_tasks[:]:
                if task.started:
                    status = task.excetion_monitor_and_retry()
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
                    time.sleep(2)
                else:
                    task.create_and_start()
                    time.sleep(4)
                    status = task.excetion_monitor_and_retry()
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
                
                if status[0]:
                    # print(f"Task {task.dt} reached terminal state")
                    print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1} : records_processed -> {task.records_processed}")
                    passed_dts.append(task.dt)
                    if task.records_processed>0:
                        push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                        print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
                    active_tasks.remove(task)
        except Exception as e:
            failed_dts.append(task.dt)
            print(f"ERROR : {e}: {task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
    return passed_dts, failed_dts

print('##########TASK03-UDFs-DEFINED-COMPLETED-SUCCESSFULLY##########')

# raise Exception('fORCED')
############################################TASK04############################################
print ("STARTED TASK04 RAW INGESTION")

if FILTER_CONFIG == '{}':
    pass

flow_configs = []

source_target_mapping_dict = json.loads(source_target_mapping_json_string)
sub_tasks = []

for key in source_target_mapping_dict:
    value = source_target_mapping_dict[key]
    dict_element = {  
        "sourceFields": [  
            key  
        ],  
        "connectorOperator": {  
            "SAPOData": "NO_OP"  
        },  
        "destinationField": value,  
        "taskType": "Map",  
        "taskProperties": {  
            "DESTINATION_DATA_TYPE": "Edm.String",  
            "SOURCE_DATA_TYPE": "Edm.String"  
        }
    }
    sub_tasks.append(dict_element)

for val in filter_values:
    source_flow_config = {
            'connectorType': 'SAPOData',
            'connectorProfileName': 'GW_PROD',
            'sourceConnectorProperties': {
            'SAPOData': {
                'objectPath': ODATA_PATH,
                'parallelismConfig': {
                    'maxParallelism': MAX_PARALLELISM
                },
                'paginationConfig': {
                    'maxPageSize': 3000
                }
            }
        }
    }
    
    destination_flow_config_list = [
        {
            'connectorType': 'S3',
            'destinationConnectorProperties': {
                'S3': {
                    'bucketName': TARGET_BUCKET_INGEST,
                    'bucketPrefix': appflow_ingest_prefix,
                    's3OutputFormatConfig': {
                        'fileType': 'CSV',
                        'prefixConfig': {
                            #'pathPrefixHierarchy': ['SCHEMA_VERSION']
                        },
                        'aggregationConfig': {
                            'aggregationType': 'SingleFile'
                        }
                    }
                }
            }
        }
    ]

    tasks = [
        {
            'sourceFields': [
                filter_field
            ],
            'connectorOperator': {
                'SAPOData': 'CONTAINS'
            },
            'taskType': 'Filter',
            'taskProperties': {
                'DATA_TYPE': 'Edm.String',
                'VALUE': val
            }
        },
        {
            'sourceFields': list(source_target_mapping_dict.keys()),
            'connectorOperator': {
                'SAPOData': 'PROJECTION'
            },
            'taskType': 'Filter',
            'taskProperties': {}
        }
    ]
    for sub_task in sub_tasks:
        tasks.append(sub_task)
    
    if val == 'all_dates':
        tasks.pop(0)
    
    trigger_config = {
        'triggerType':'OnDemand'
    }
    
    flow_config = {
        'triggerConfig': trigger_config,
        'sourceFlowConfig': source_flow_config,
        'destinationFlowConfigList': destination_flow_config_list,
        'tasks': tasks
    }
    
    print (flow_config)
    flow_configs.append(flow_config)

flow_tasks = [AppFlowWrapper(flow_name, appflow_client, flow_config, max_retries=3) for flow_config in flow_configs]
print (flow_tasks)

task_queue = create_queue_from_list(flow_tasks)
# passed_dts, failed_dts = monitor_tasks(task_queue, MAX_RUN)

# print (f"passed_dts :: {passed_dts}")
# print (f"failed_dts :: {failed_dts}")

# print("All tasks completed.")

# print('##########TASK04-RAW-INGESTION-COMPLETED-SUCCESSFULLY##########')

# job.commit()
passed_dts, failed_dts = monitor_tasks(task_queue, MAX_RUN)

print (f"passed_dts :: {passed_dts}")
print (f"failed_dts :: {failed_dts}")

print("All tasks completed.")

print('##########TASK04-RAW-INGESTION-COMPLETED-SUCCESSFULLY##########')
# task_queue, n = task_queue, MAX_RUN

# active_tasks = []  
# passed_dts = []
# failed_dts = []

# loop = 50
# outer_loop = 0
# inner_loop = 0

# while not task_queue.empty() or active_tasks:  
#     # Start monitoring up to n tasks  
#     outer_loop = outer_loop + 1
#     if outer_loop==loop: break
#     while len(active_tasks) < n and not task_queue.empty():  
#         task = task_queue.get()
#         active_tasks.append(task) 
#         # print(f"Started monitoring task {task.dt}")  
#         inner_loop = inner_loop + 1
#         if inner_loop==loop: break
#     # Process active tasks
#     try:
#         for task in active_tasks[:]:
#             if task.started:
#                 status = task.excetion_monitor_and_retry()
#                 print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
#                 time.sleep(2)
#             else:
#                 task.create_and_start()
#                 time.sleep(4)
#                 status = task.excetion_monitor_and_retry()
#                 print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")

#             if status[0]:
#                 # print(f"Task {task.dt} reached terminal state")
#                 print(f"{task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1} : records_processed -> {task.records_processed}")
#                 passed_dts.append(task.dt)
#                 if task.records_processed>0:
#                     push_to_structured(appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
#                     print (appflow_ingest_prefix, appflow_structured_prefix, flow_name, task.execution_id, task.dt, filter_field, source_target_mapping_dict, TARGET_BUCKET_INGEST, TARGET_BUCKET_STRUCTURED)
#                 active_tasks.remove(task)
#                 print (inner_loop, outer_loop)
                
#     except Exception as e:
#         failed_dts.append(task.dt)
#         print (f"ERROR : {e}: {task.flow_name} : {task.execution_id} : {task.dt} : {status} : retry -> {task.retry-1}")
job.commit()




xxxxxxx

%PDF-1.7
%âãÏÓ
10 0 obj
<</A 11 0 R/Border[0 0 0]/F 4/P 4 0 R/Rect[220.95 219.88 315.71 233.3]/Subtype/Link>>
endobj
5 0 obj
<</Filter/FlateDecode/Length 3120>>stream
ôÞþ«>°F.í¥@¿Ó(#§%jTCàÉ,ÿ¨ŸHa*R£¥sØ µ-ôÙ
&Í¹cÞo3¤‡¥~ƒÕ^¨wô<Ã8M_2¦÷‚å°ýPðÃÃLœaiÂˆÒw÷%XÕw>åÍñÃÑ"zùó' Æ?n:m¬S¨©‘æœ¡é>WÌÞ@ÖÁoÕ¬ÃAÔçq‘ª|jöWµD9—¡þE"Ùö[H]Azâ©.Ä•Äj=£ä½&¸kZåšÙsŽßÒK>3>p,¥Àkâ½’ü¯èÚ~£
[‡,©EÀòK3íÄI9 ª
¥¶7õSoL'¶å"P^¥Ž2+…:8xz·´lìçÖ‹Qhõþ`‡Ã0Ç‹j³ªrãÑI¿äÜ¬Æ¹ù3›é;±¨³ëóàÖ2x’	“ÕOæÁBgÞgD–ºÔr£âÅ.q#à¨ÄËG±W‡"ÇòþD•Œ³m.0@Î2Æân·‡Å–i‰â×V_Õ\%¸…ü‘3UZ!Ü¯º\óÇOg4¹­ÈêÐÿè
 ¡åM)ÐÎ©Æ§þbZÀ.n¡
šÇ�ÄZ€‰wEµ±ã­€*.ýy¬¥ÜâNÑêbƒœñž/ë¾Ë	CÂCK!›™Šh¹>ÉÚ¼˜Úî;,1œ<ewÁ¡ƒ‡oÍø äqVœÉQb=,¢Rÿ¸A_áúžû¦J…fjÓi?ìÎ¥õeþÈÁòHS”úåø±ŽÑ ^Ë­têë¢#$æ#±&4•5¾Ó,E³Þg¯Ÿ1F„?ÄþÑ?F.Þ>Jû¯r7ë:0ù¹gtè~¥¨igŠÀÊ±˜™²ã"X¤j	³…½¸1Í«lómŒŽÍÒø¼ã˜«4±Ui‡«Â¢}%O:H"È±äÙÖ$¸`Ò°)-Z¸	W!(w@ÇÛt0Bó§W©bJg¨?6¿)
8X‹ÜªöüAôA $y€
ÚìW¨j±N,Êg²Î¯vûn÷1ÿ—7Ð(7'<Šõˆ¤s±ã=¥£¨:(Ôa¸Ü#gF/žÞ\OpIŒžø}_%²ëa23¯°Úƒ&M$0°ÎŽãÔIö'Ç»ýß©,NåÇ¸°æc¼Ô¡—lkt„º;ªoÓüã1bÙ¾È":PÓÒjV	¯F™¿Í¦o_† (Ìâ"�¾¦æYÓ•¯ìòIt¡ƒ80…†ìoIî":R>NÕ^ìkÏ¾¶W$ê8/ƒù*9&«É7ÔËGr–þ<GÀ¹>à|!ãXZ?£_—‚¿çÇ€ã†Ô/5ht`Îçæžîy`."˜®Øw%.Âlð›q¿E$*Gbªã‡bPGA¸VA„ÀÄQ½ÓÔ% —ÜF	ê0HÀonÇªÒçI-ñÚ\•Æ¡	œ«Ñ|9[éV‘W†$@²ÐÇ#~ÑÄhÖš‹=½ªrô±*WÝÆÒ8Çaµv”�ýÆÞíÖ±²¥5…PõÐF¥S
@²õüþy1w%¢­ë3†Tªã¶Ò´Ä&ì{xÊF6”ŸG[O	ä!Ø®µzqô ;RCJ¥
Ó–€ÇFð*Æ·kÖ–c+ O6ä$•lõÂ0€üCÀãä°LFMeÌ†½ûŠÑ†Q’Pî3FoÆ‡¯¾ÀLê#»?×øìÄôõ¢ÅF„ûeÚÚ€\+¦ˆÙF`©—œ‡×ô
â“ŒU[ÍØI…B.~Çø4•n?±®RêC¡íðûÂ`Æ™&GžÃ¬Y®·¦fÿg¦&ÛÊú¾;\p9w Z]n#¿©;wdž9ùÔ;x®‹Ú"	‹põ¿HÀZƒAs) ¨k>ØÅ†”q-Ï{-««tN´Õ3JR‰!™æËÍ;dþ¥•ºžbª³|3Õ×ÓâõjõYb9×k8-„9ijZ\IÎ©åqwï€ÉèhÙ,ú)žàÖK)-¶h™×ãe_wD×ùÒ+Ã7ßÈZH…@Ä{ZœçÊèØö%ÄÄkçöŒ’iÛÙýy©Š1`VV¿‹Aý´Ú|#¢X#ÄÁ*§P™Dk°	Óìö=‹‡GWÑ]Ó=cfvâ´Ç†Û÷)×g!’lÐm:þÔDÞ^åÓv|\ºcí/ÕcYaï­V|ôîªuÛÏã–ï¬@”¶ƒçðÓ�àäP/LÖƒ±Åìâë´ÖÓöÃA–Ü›Ã;>	ž»du8VÌqžuA3°¦á
EãUÄôC¼`Àa˜²íàD�:^h*ÜtëKÁ@×Ž_§e¶4¡lã¾Êô€Þ6+NZ'À.#FÓù,Œ£ÙÊ¡Rjùºš†­’‹]ÎŒ»šEÏdY„IMqˆ¬ç‡;¨è÷œ™{&¾wöš Hväv@Ùÿß@0é^Ypx[Ù\«„.,¯x^I¢21ôË[ÊŒµkÑÜ¦_‚°HÏí%Í²ÝÅûÈžê¨%PŠÑÕ'|oò›1ÁÃÿ0ÙS˜ßú BÉÊ-^¨9Û5äŒ¼£êµ7d7¤Ã×ÿnØöbAÏùr¼(ˆÍõ}h�||äÓ“‰ö"qÎÆ=Ë³"=3U‡áÿ÷ÜðxÏ‰«oÙµ;TÓãÔD[²p¿MÆ€ÓÜ,‰
luó¢Œ8iZ›4»:˜´÷iÛçZ9ÔÞÎ5ÄYÀ|ä]ñG¯†#•V9Däÿ)~6þ¸	åùbÝÜM€×û§J`Kö|×³ïRXâHcÅ1Ž_7îšì¡d¶Å¦ÑCØŠ¢8¦g|{A‘AÈ p?õëZ˜ã
Þ­<éäÁÒqU¹k4Y ”^o-û²I(±óºM2¯‰µîegVŽžÅ9Ÿ‹wx‰=5/¾Û7ˆ/Îãçÿ˜Ä‡¸
9ÂÀ>Ùz~P†T#ýØU‰ªše3×ç’>êÁCè°qSÄ¬rØ~`yŸØtC"ÿÉ8ípÔ"Áz§uMQ*c–©œ1š#Cl?LÎ?£ÛÃ ¨`Ñ2ø“Ót'ÖP£Ä¾I"—	ØÅÙ½wbQÊ4…WAðT|ÓA^Ú/P|#SD£ƒwU¹‚ê
Êf@ðîŸ2&íX{vŽ”»À9Í\ÞdÆ÷/£C^Žž¬9V»Y¾-pNPúlô%hzøÜa1œSÁúïð`DO³…Àû…”i?oR:z­á@¡"‹T]mþÞ°:¦(M¦x´I)«H´ Ù¸�K„äõ¢»­ñ“¶Iø	ô,E	6%Û¶ÜËÂúQ«Ynð=·!©nÜ"<’oe5››ÉÍŽÚA2B.³÷„‘Ãòò�Ša')uXì"èÉÖ.6¾ÅÁ% ØL<‹†×ÓÃ‡ÎÿatS²:¼6û.ýß’HOäa8Låpzn‡¥NhÙÍ'¥_-^ül+l›¬üi½@hÉØªÇ@¢‚¢×"dYÍ|öúsI—+vÿ6|O+yMAæÊ€
‘7I?ìdÙÅZÜ%b>›ÔµÂKÐ¡äÝÆ0ç¸$qŒ.ßV#ÞÄ(‘>ºNv.Œ©¼à5Œéê:]q,-î¨oÃ*Ú9žü²”G]^úÙdi îu6R³0í^`¼SãûµÉÄ '^}H{š÷·Ø,þÖHy{È­„Ì‘ÿ7ÓQb&N¸ê«5Æ¡jãpkáLaFóA¾úy:VØG§]ìÔ%‡y˜–¶|ðÿxá'Ô4†¶Ö“ îu ZÐÂ‰Á!™…¯lŽrÿÜI¥|8à“Ýqžñ^ÈI'¼I8ðb–ëP*Åá�)£èž.ð õgBh§]–Èå?¾ÂTûª“
nþâ“Ò¨+I<+÷„zÁ`
Y$¯`¦•`l^w¼s)^‘"µÞQ[×Þ™9óíòø¯«¿)óz"¨p?ÀÊéÎ8‘¹¼ía½¤œë¦nyAe|[Û©Ààÿ,-·Ñ0tubI„S»^$PØ+ÖÔã"aš.ÓˆänMÜ3½ÑµåfK:¦31}!ÒdRï‹ùGÕ¤±£'ÛdÂ	˜@ÌyöN¸»³
ØÒf§æ,EgHS	,xëlôŒ8]xd(8¸³Š2Å×1
endstream
endobj
4 0 obj
<</Annots[10 0 R]/Contents 5 0 R/MediaBox[0 0 595 842]/Parent 2 0 R/Resources<</Font<</F1 6 0 R/F2 7 0 R>>/XObject<</Fm1 12 0 R/Im1 13 0 R>>>>/TrimBox[0 0 595 842]/Type/Page>>
endobj
14 0 obj
<</A 15 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[63.21 672.98 157.89 686.4]/Subtype/Link>>
endobj
16 0 obj
<</A 17 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[157.89 672.98 160.66 686.4]/Subtype/Link>>
endobj
18 0 obj
<</A 19 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[72.75 493.36 108.78 506.78]/Subtype/Link>>
endobj
20 0 obj
<</A 21 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[247.54 493.36 282.26 506.78]/Subtype/Link>>
endobj
22 0 obj
<</A 21 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[72.75 479.24 103.24 492.66]/Subtype/Link>>
endobj
23 0 obj
<</A 24 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[42.75 327.75 95.78 341.17]/Subtype/Link>>
endobj
25 0 obj
<</A 26 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[319.29 327.75 462.77 341.17]/Subtype/Link>>
endobj
27 0 obj
<</A 28 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[462.77 327.75 465.55 341.17]/Subtype/Link>>
endobj
29 0 obj
<</A 30 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[42.75 313.1 258.71 326.52]/Subtype/Link>>
endobj
31 0 obj
<</A 32 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[343.26 313.1 393.87 326.52]/Subtype/Link>>
endobj
33 0 obj
<</A 34 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[399.1 313.1 499.26 326.52]/Subtype/Link>>
endobj
35 0 obj
<</A 36 0 R/Border[0 0 0]/F 4/P 8 0 R/Rect[115.61 297.92 329.31 311.34]/Subtype/Link>>
endobj
9 0 obj
<</Filter/FlateDecode/Length 2944>>stream
†aZš³[ŠM†×r†+ÌÛF''¿6a?•§‰ß1ÝÐ¹ˆ®§
Ïhy3JÎZÂ'*=%Ž|;w¹®N‰¼³·çÅìøÒ+0É¢é_'–)A¾eeéý“—f˜¢¶ÔåwžÚH'?WX˜f÷ �ži’±
e’¢mÞÓ]¿òêÁA¦î‰|]9ŒP@éù›u`®ŽØ=e›â5ÈR�41AbÖ‡†³`rÎýÅ¡4×º#Æ¶GùQeÎë>¶i8ï—*íYîƒ¥3­øŒA-7E‚Û¯¿øÆÛ¢cG¸µ,ÂRö@Ý¿A££]K·ï¢=¥GaO‡];“òfwl„ÿN÷1”Ófq½ð;Íi<íëžÕõ‡ŒÅ.2;Ù«#Â×UW\¿àÅã?÷z"ÝEðì9¤ò6¼%§,1Bå3ÞÚsï$ê8Ë(#ª¹áÚ
YÓX’ ° öWU×yƒGƒ-A³©ÿyÐ˜gÍ©¹QLõ³UÙî2¢æ»FÅl™§nA(»OžŸïø5>ÀçüÂ:zä¸„‡–-®X‚èOîJÕÉŸ¿þÍ~&ºt*œ%Kâì¿vrg?â4É;YqŸ*\áŠêvsP
2d±Hê>éÍa7ãOã•ÌDñ7%L4jÀ>Bx×ÎR.süÊ™²@ÄûAöØ)Í/buË
œÍk<[¯MÎ‚=¯(º¤$�2ÓN:·TeoðÏ’‘LÝö­?­8
JÈmryªåkóc®Ê.Lú¶¡qÑßèúÅ¹ÑOÖõÝ¦3¶>§Ê™Ñ{é
¸ŽšÉj\à*æGl§¨¿3y‰Â¯D/[á“ŽEìä«¿$òzÀ
£•Ô®_BKhæœa•ß&'¸íÛIºÊ·¨mŒðéë
ŽÚIQoõm6é.&œöi�´”Eó™ U�:²î®H!QŠyÑ[Îs¥ì´
¥—ä%svÌvlÛ	¾.;öQNmpUUˆäÿã |òú’GãyìWäPÝ«mŒž‰2­ûAuñ vn¬-¶îX 
þÍ‰áaŠp’ŽD¿yÃØØóæZ‰ç—“Ùï€¸÷é.›·ùí–Æ½ªŸsŠ£ts°Ôý$Oá´Cd¢ùÛÊs!Æ9ðQÙy½`ÄÚÌx:Ë¦6ÇÈAÄ…d-¥Œ®RæS3h´1
Ó'6ËJCï"z6û{Ú¡]]ÕŠ“‚rÅÚ’|ŸAÔÏû ©>«Xü§æ|!FW/¼.–·µlÛÉE<îmÜz”DI íóqü³hˆŽ"âHÀ¾bºÔ˜H•£
7QT|ÒÞ25áûtfç³{ŽÝŽ¤êÍ†¹e˜˜=Ò&§C
»×—-²´¤î(~ŒWÞ¹_4`Ú™=jyÔÂìè!¦í¦H"ÜÕÚ˜¾·¬§E`˜¾×Õ”œ~Ç3ŸWµ’¤Ž¸©v>J´MÔà˜¢¹˜˜ç@t§â„1¶‹ÿ¹3QŽåªÚÚ÷0ƒ}ßÙ˜XW·©™zcB:ð€Çh5„~ë“vŠu1Ö…ßÆD™©SÔ:áJ3g´u?cè±q¯cÑä.Bãü«ò}VDÎÏô	o³”V)ª 9ßòäÏÓ Éa¶lVoèÜÛúC×m?å˜Î‚¥3ä¼ù©íàU»…
³Ôú^ûõm¿ÐžÕ³­LëÍ%¹.ZJ÷ÀÐª¶ãX¯1¥ÙÍõXz&û €ý@c‡±ohåüÝ8uÍý1þøxô´àzn<‰ê¶»Y›áÉ©p*ª`íÌç}pÐomSDOö4Ïù¾önôÑ¦¹s6È9†·úÃÁ{!­[ý;—áóÎ¢W wÐóW;ÿÉ1nz"+nÊÅ#"ãŠ¥É¨=ž;Ü#Üâô#áãRf£hpµø¯ö
ýµÔ-”MúÛ‹r;-¾¯…\¦“àÖë¾ç<Jº»JB“¦¡rÄ€ÀF<Ü¥éº÷¼Ó½:ØgÒN™K~nÌ Þ"Â$$ý2M÷9Ú¡
¨€õ3ütÛ] Ö�&bzÜ+vÃB[�mÚkj+¦Îz?4~(¦—ó·Ÿdma¾ôóùP™q×„t™œi°û—èâú|>Ž¯ÿI4¼Öxl¤f7èŠ,‹X7y?’{ö’d¼óÏ™/y]QU¿ DÁG4Þ®L”×
êÜÚq¹õ&Èíýkiƒr±I?f{Ê�£2hÎ˜òµ+[ %…5$–_µ‡O¯ª®¬wDÂ•ë™ˆfÏÛ‹9~ÆáD\½±sTõuÑ±û¶ˆ‰]¤nUüà
épØøØü¶¬aæ)í1‡)¯CŽç%ˆ„_ºž³¿Ó·¢‹k©TÇ…ÐP;a|Žšþå°ƒe2+D‰l‰ýâ÷HT÷2§I ggÅÙ
c>æ-‹
}pÉd»¢ãüÏ$<$½:Ð@yŽ…¦õà‹Si°ÄœŒDÙòÚ#ú„lw?WA4ä
.Š”°öZ#Hv¼ñJoà'“Pf¶Uf)x®ig±ƒ°ªœœ‚S%ÃbtuI\àÜÃKÓÀüçVsBŽ¯RS º½P%Ÿûfò£»š6ôày¤X,¦ˆý6t!Ð¯qXÉ‰zº)+ùÐe_?Žð£Db9ÕzêÒâÛzŠ9‘¨©Ûòk›Ž…×ùË¾TW•ÒY7-…&Xvß€D:U$©Û[Ÿ¬¿ÆŒ­p—8¥¡pÁ¤>ó€Œ€h‡‹žùsÙ94Äc;”$MŒß|ø£×œ æ7~®F½^±¼Þœ&áé´¡V4w4bO¤QR¹ð%í—sÞi–i­HåjØDäMåÄQý…i·e> ÷ÏBÞ}»ÈÂõ‰ÌÜý-¨Ï¥Èl|ìwû˜Ã^û;ˆX¥!z~Ïî¬W£„…?‚ŸŒj\Lf·ñ#p\$¢C€¨`Pqh™ N¥kH”(ŽZêÏû­nU3WFB×‘º‰Š]ºËmš	op¤ŸgËÊÖ³$‡áõö1Îáµ¼‹þç»é�¢-þ­ƒ2›‹7ÅØ†ª¸™‡äÄVøÂYmÍð/2šEÂBJÖîfLR#„Ýmº˜Ê£5-Ÿy¦ö
­ïŠ)ú¿¦iéU9hHà›þRµ†æ'!û0Ù®˜ØœuÍdîÏ|Cy]ÆBKçªt°ÖvAê¢S{k,7?d�ìU©?H¦[Î£(ƒJ«‡<Á¶b  0(ým¥ñý–Ñ€ÆÎEŸtÒ®�¿}õeS‡×
¤…
u®·›!üŸÆ^‡šúÉ /l…ÐÂ¦s‡kýg¾žˆqæ—þþÐx6Þ ¦@=i·YÈùÌ¤Ñ�š"*v‘mXšØBíRÅø¹uñáü¹;8éÚeu±£3G$¾Dãà65.à j•Up{š°ƒÿöúq:Ì·ŒŒ#FÆWÐªPR³ë5°í}„)&VdF‡`Ü7ˆ‰¢üçÌÈW+øÖã²W§nG€…,‰˜>6d¹ºÏì®rï5Z]–eBâmâë¥È¬âQ}·÷¾Îë´ßÏv‘ÈÜT`r½ÕD±Öü3ÊEŸ€æ„wû‡Ö^Àpç¬H–
S&$š[i­
­Ûð*˜Õ\·ÏÐ%§ü~ð Z$r+•ˆ¤[uýUi$¿y¸Õ±ìƒ'Mo––¨ßðW(¤¡¸“„<‰.Oó‡Îª°§e9)x†~HR.WÁ¼<"ø7ùá¾D¡D-ï÷Š„ô‰oUiõã%½ÃÌ	Ýxp:Úcj>ÊÚ0ïXššì¾Ô@ÀÐ§?°»Ž.q[Ô(ÝI¡|Ò?ÔŠ.`~ÇßOK¡Ï/ÒüÓ“ê/}Ù@F¸Î•ÈÛ
endstream
endobj
8 0 obj
<</Annots[14 0 R 16 0 R 18 0 R 20 0 R 22 0 R 23 0 R 25 0 R 27 0 R 29 0 R 31 0 R 33 0 R 35 0 R]/Contents 9 0 R/MediaBox[0 0 595 842]/Parent 2 0 R/Resources<</ExtGState<</Gs1 37 0 R>>/Font<</F1 7 0 R/F2 6 0 R>>/XObject<</Fm1 12 0 R/Im1 38 0 R>>>>/TrimBox[0 0 595 842]/Type/Page>>
endobj
1 0 obj
<</Pages 2 0 R/Type/Catalog>>
endobj
3 0 obj
<</CreationDate(\020’Å\027\f3Ô®\020 lg†lÁ£\(Còõ¬çTqT ®uC7²Ës\016]F\024\003\023\033/\bsÚ¸4\020’)/ModDate(\020’Å\027\f3Ô®\020 lg†lÁ£YÖÕ.RÆYÎ\fÞð`u¬põ\005‹•áhúÇäqs[äÁ£§)/Producer(\020’Å\027\f3Ô®\020 lg†lÁ£ÕµDfisÁ†\003z\006\024Ì\027@'d]ìG"þŒ\005Ò²K‡Þæ€ah¦\\Ÿ|ò2KÕ\036\005±I7_{epz~\037z\too»>Û‰º\025\031•TjdI¢—õ\001Þ\035\024ô‹ât\)ü\026\030—•tcî÷\024\r0VÂÂ}‰ñÜ\001­•I…í"$\\æ”vúSÜGLÂ\027\002#ej\017eƒÿ+Ö9\003ˆU¾õ ÒÌ*\006F–qyþ\001\026¼Vt¸¨ù2’á·)>>
endobj
2 0 obj
<</Count 2/Kids[4 0 R 8 0 R]/Type/Pages>>
endobj
6 0 obj
<</BaseFont/TFPAIV+Calibri-Bold/DescendantFonts[42 0 R]/Encoding/Identity-H/Subtype/Type0/ToUnicode 43 0 R/Type/Font>>
endobj
7 0 obj
<</BaseFont/HQXYDH+Calibri/DescendantFonts[47 0 R]/Encoding/Identity-H/Subtype/Type0/ToUnicode 48 0 R/Type/Font>>
endobj
11 0 obj
<</IsMap false/S/URI/Type/Action/URI(ï•ª\022¸ë\006Kµ\t°*\013ãWL_WØ¹à»”â_\)1\006"Ì$ÔÊæÐr\030\016f\000ËUµ6‰Ï3õ\r”¨K¶=£•\034¿Týx\000©O\(0£=\036@á\001¿\000\rÇI\002‘^j©åoÀ ?K•‡e”²ÿ˜,âè:Ž?ò×w\000¬ó¨¼ÖÜæ\\ãy¼I,ÍÜÁÃºt,;Ô\n²õ?©8ÏÀ¨‰Þ½Uá»Ë8\)ÿ„ˆÿ/‡þå\007eüÁ\r¯2×|_ŽLT†3\034ÇmcLò~gÄÀÞ4\0220vI\022]œ©W'×º×ÕÉü\(É!—#)>>
endobj
12 0 obj
<</BBox[0 0 5 5]/Filter/FlateDecode/Length 32/Subtype/Form/Type/XObject>>stream
–°@X«BIC1?˜Íúz rb™óþ–j9s0¤ŠÃ
endstream
endobj
13 0 obj
<</BitsPerComponent 8/ColorSpace[/CalRGB <</Matrix[0.41239 0.21264 0.01933 0.35758 0.71517 0.11919 0.18045 0.07218 0.9504]/WhitePoint[0.95043 1 1.09]>>]/Filter/FlateDecode/Height 97/Length 160/SMask 49 0 R/Subtype/Image/Type/XObject/Width 150>>stream
dG¾çŽj
ˆ8*kPñ°xÕdn ñ(æÙ'‹˜0L‘ïþõÚ²ñË}ƒ<éo¬®žîï*Ú‚Ö;Sðv&‚tóv^èª›F�—Ÿï¬züvèùð„Þï®
ñ˜Ò2¸Bö”GäYjgÓU(‡”1gMà¬M)ËxW�ŽHHËÔ¥ïO|”ësÖ‡R@)d¨é¹ñms¸bèCª{V6
endstream
endobj
15 0 obj
<</IsMap false/S/URI/Type/Action/URI(ªõmÀ1.²íÄÅ#\003\004„\032\031—Øˆ3ÅDß?·Ò¹u[uìpÁeñìHaß\030Fìƒ>Ò†k°H­ë\000ãA\0023Õ·\016¹šI³¼E…‚2ƒ°ÒyŠ\013_‚æiÀgŠêNü6¤Ì\020&œdà³\020ÆÎ€|^\024Ð¦T'àúè±_)>>
endobj
17 0 obj
<</IsMap false/S/URI/Type/Action/URI(`y\004€$xt1ž=F/U1\nÒsþ¤Æ¯¯\0322£\030wÆ\)0¢»áö¦‡W<\007BˆcwYðÊ€iô a®:<z+\004¼\023c\024kŒ„ß\036\035V"-\036½m$!\(\000pJ/ïËw‹°4ù\tñ\020°+\007ÂvÉ\t99º¥^\017?5íÛ×\037)>>
endobj
19 0 obj
<</IsMap false/S/URI/Type/Action/URI(ÂgÑ è\022\024b\020‰-\006× ;J5Žy®ƒ«á¾>¢Wåô¿ÕChd\(Yê,¹œže`5ciÂ0:¬Ï<ÜNjí‹‚*\006›g3a™ÿnòzÕcÁ<!\006Gµ­±)>>
endobj
21 0 obj
<</IsMap false/S/URI/Type/Action/URI(½…¡ÿ\021\031¡‚«Mø\030\)Zá—,Á—›¬ÒjºU\003a\000³6V2‚ìlÚÄÊø\\Dú\\\000¡ârì‰óž\016ZdWtËþ\035 ­$2)>>
endobj
24 0 obj
<</IsMap false/S/URI/Type/Action/URI(¢‹ýÜxá¤.{Õ™ÔúÉ\035\006å¡ùÛÇ÷ˆï5aÆ]\rêŽ³\004Þ×ä÷–â„ ”\022\(Y\035V\017ÀžâÏŽj»D%f¦íoÙWÜÞE\t¦5á¬\013ÏÌ\)¢äˆO&p'‘š\002ú¼ž»ˆiã×è~a\037˜\001a;\026Sb,–\fÊÓŠS·G\020Ôä=Š&S†‡.F¦ò£óg8Ûþ–‘£†•LY\017*¯¾«±¯&Cá9}v¾¤ë–€‡,\033ûkWñóÒ\004õ:ã\b\017OTß\035úÑqÀ6.Ð}ò\\ôè‚ýW¦\020Ä0mŠ¶ø \025r<\001CG“ªqõ´m`ÐyþkF|Ž\020ñõ\006\032‰È\034n\037ëû¶\tÞ¿ÓaÖE¨X¡e\003\004ùêþÝÒä,^‚†\034“º2vjE6û} '™Ýü×\\\033\032üÝamL\031en§m\)\003“²Ã¥ôò±‰\002\003)>>
endobj
26 0 obj
<</IsMap false/S/URI/Type/Action/URI(">ÉÕI½\\\003\021fp[\000©ç{KŒü=±ë9\nÖsºØÒM«ž^\007•²\0270ente\027aïþ¡žÈÚpoMÙæÓŒbæ\001*\r×X}Ÿ©mÑ³\004GfÎ¢V„A0„æý\)ÿÁL§ú%Q9…—\037ÿ,¤j¶œµÜ‡€ÁB¬ë>TR\)öˆ\035-dþV<÷§ogÞ\022µ\004;¿6»àA-rg´9áƒi™\fÖ=ÂµRøY\024û\034³Væ\037¶6ô\033…ìë\b\007lwnO\002%ŽîêÝ¼áT&\030æœ\000^­ð‘óê\032H\000¬g½Õ\037R\037¨\030p‰Ó¦\020~¦\030L[Ñ\020S‹U  \001œ´kìmr¾Í»÷ö{I¿Œ=\025²Ç»?ûio\tD\034zTø\017%\021”w\t\027e÷‰\)uÝ³†ýb\022$Š\004¬®W\003œR˜Hâf§˜Æf2K\000:rUõ¤Vïåß)>>
endobj
28 0 obj
<</IsMap false/S/URI/Type/Action/URI(*ß*”\024A\n7\005\031#ð8ù\021àÏpÙÈ\025ÏE„¥\f¼\026\tKâóŽãÃ\005©rî\023\037Ÿã/&F{Žµl|ýu&>\013\025a—6\033®ÐPjY ™S\001ýžÝ@Fu\027¡€4O\035›áIíF—AðrG\023Mª`Ex=Þ\006eë;³Ô>Ñ‡À†1þtÖ;ðW¡Œxe€•†â´çS®©åÞù\017\017ý|È:“Ðš\)BÒ«£Íç\031x³*B…ÑNÂ®Ég\tÖ.œ¤D\f€)>>
endobj
30 0 obj
<</IsMap false/S/URI/Type/Action/URI(ýÌ,zO"®Uýó}˜ü\(G€!\004Ä¾øôš.u=l\\ZÞú\022o<®ÊH$C\\€–ìXß\023àk\002Õw Œ'¹H\016‰¬\016Æ\017\r‰\0040\034„÷½Üo¡°\001_HË*\006Í–\026\021\024•¤<Èn+£@\032=}k\r›\023Ùžþõ±&ÊÑ4Y7wu\037ûŠ†öDÔ„GA\034=>bûû¤\004fâåÄo¤q…WêaÀ\020oGZØÌÈ•‘¬jïE¿t4Ôè”“”þc²WbUÐ\025÷¬p¶‚¬ý29°ãÙ\024Ã‰0&óK‘ñ©ÿaŽ±ó1ˆQ;\017ºWy\)]n\016LS¾´¬;\030ÁyËf4¸`›Õeò=­\004Àft\034ÇIU‹X£;<Ô"‚€VÓ\031M­\033ô§\005tÄâŸ]\023ÅS=Jä\036l§'Û!7Ø\004®ÖäMËú&Ï¬ñY\0077\004"\003\027Ö\026J)>>
endobj
32 0 obj
<</IsMap false/S/URI/Type/Action/URI(@-¬\)ÐFjÎÅ ñ]•­tgÍ0ÊÂ8\030í‘%â¹ëY\037kÔC´oóµÏ\020F^KæD^%É©;\005%G™êQrAƒn­u\007\004^}\026Z}3XÎ\003†«9\004ÍN¯‘W\025'Í_0¤ÒÍÊ‡W\033Î—ÿ§räs\t\(Ê€F€‰\027kx`–/{Y”ú.‰òYÀ\027™àI\033\034Þ`=A9=Aj\0313–\033:4.ã¿\r\002~…r\025\013\036ï÷ÈxXë’áZF9\t‘f\002HM'—\0271§Tt\006Ô¼BþrMÁ$_m\)T¾²`Þ\025Çñí1D9Þ›2ú´{Âû™À¸\(Â&ïmJ^\036r4Q,Bw@}Í×±;x¤:há‚<\017\024^+\017¡KõÏP¦ €Ò”\023Ðw©Œ¹I1§…Ñwd¹ã‘í}ä\(q7º,C!\031\\@hmŸ"ÎÞR€HÎó5õ¤\034c-4„:LÒ Î\f\bÏ8ØK]þÁ3ZÁ@ò‡”¤bCYƒØ¡\nÑkBÛaNIø«Ü!‹œ0-¡VÜ\025U\(d&°eã\000j?%\005)>>
endobj
34 0 obj
<</IsMap false/S/URI/Type/Action/URI(Ä¦Ú#m‹5u![ò}hÜ¤\tßeÁµ¤/\006ú¹\fòæ2¤¯Hð˜þÇž=®‹Ÿ\\\004µùÌ7{ ¢:ú[šÓ\037ñ„2°ì} ]ÝœÌûþ:Éø\000€Ñ¥¼\)\fÙQª\032ËŽ\032Dfi\035C\031L\027WPÜmuÀ©\nsTã \030’YÄ™µ\025É\bÿÆÜ\023Ã¥oþó\017“ÊÈ4¯Ú¿\032@¶p\021x{Ù\0354ì\(¨\001\037Kû<—\033\017l^N/ur\r\026r–^›*E–çú\002j\027§|\bMÂ\004­\006‰¿\020ó6œ÷Í%Ë\024\r·<³-‰F5å–\013§CG\000ùT¿qË\020ì…*#\027"äÞÞ%\016D$­‡³Ð\032473ToP@-I©^™O„ÊÿáD”Säª{c‰|°!5N­ï\(íþÏ¬üœ6'$áqùl—\032\037¢ÍT¾¹œ®;H\035ý\004‰\fŠß†¾\025¶M×©ð\021lAôÏÿ½“.ºè‘ì<\013\000\nûãÛ\005ZèÙîäíž*½î[\rG²„æ¬\f’\t H6ðw•Ú\\Ò:W¿ÎÐã¦Àè\rKìÑ~’\025·$ªkqC)>>
endobj
36 0 obj
<</IsMap false/S/URI/Type/Action/URI(ƒt'8\001@î<S36ÐÙ@\n\013Ö<‡üí‘ÇŽ3éÝ"¯âŒ¡÷ÖÑ\026ÅÇàñö®\035‡po,?¾!R»hd¿ÊvlÉ—\004èâ½PW3x\025¸ãY9©è{ó2`¬_0\025/Z'òT\027š÷²_Ù;ÂãD\026Äé\037Ö©EÊŒŸ\022Rª£d/\0314`ÜI3«\(sbßj\013¶å¶i5AÒØ“"ÖRSs\t{\017÷—5\017’Ùz«\026\\‹\001“W²œbGeÙ\025\ny\022ö‰[\000'‡u\002õ\f@¢Uë‘÷Â§Õ¸™þ#¸eþQ\037/õŒÕ\034oŠµd“R\036@ãúpà}ºž\004Û—K"ÛÃ\\&b\002Øœª„nÏ1h‘ùO®ŒSòÕIä‘@o\(£VX\036\tƒ\b£NúVx„{À©÷m\032D¨{”ßçÐ¸¶S‹¶\tñÈÍç;}scˆ;&e\024´!ÄæÃWmÌ”\031HË\001Rå]\023á‹aÆCe¹”é$\n‹\017¤%ÆW¹ÊÙrZX¨ÀÒ…¡a—\016†Ñô!øÏ¡¶K\023Ö^#F\033B’\núx·,@vFŒû?Â1\)²¾Ïà-^Ž¨Ô¨z¦Å|Xj3`ïgm)>>
endobj
37 0 obj
<</ca 0>>
endobj
38 0 obj
<</BitsPerComponent 8/ColorSpace[/CalRGB <</Matrix[0.41239 0.21264 0.01933 0.35758 0.71517 0.11919 0.18045 0.07218 0.9504]/WhitePoint[0.95043 1 1.09]>>]/Filter/FlateDecode/Height 97/Length 160/SMask 50 0 R/Subtype/Image/Type/XObject/Width 150>>stream
éþè«l¸ÞùÃsvÅ
‰)VŠ×ÉìÝÖ¦½è?*A#²OÑ«[gø'ä1S×Lú}xÿJ¿Æ-Ôh…æy±<X‘½:9ÍÉÙ9†+`:Œx­ÁºËM8øF¥EÊài?Ïø ò˜)·nÖy÷áeHû:€3¦²ß^ZeìûiûóÌÛ"sÆ^\€7C:¸·ì-$ù.hw· ­ÃßÄ<Ä
endstream
endobj
42 0 obj
<</BaseFont/TFPAIV+Calibri-Bold/CIDSystemInfo<</Ordering(\000ç™[5˜SY\003‹x­¹xÏ¨u:\000\021\023\006Î&ŒP\036\t@A)/Registry(\000ç™[5˜SY\003‹x­¹xÏ¨2%wë\034ÕïZ¼ƒ\006{\004•Z\023)/Supplement 0>>/CIDToGIDMap/Identity/DW 1000/FontDescriptor 39 0 R/Subtype/CIDFontType2/Type/Font/W[3 [226 605] 17 [560 529] 24 [630] 28 [487] 38 [458 637] 44 [630] 47 [266] 62 [422] 68 [874 658] 75 [676] 87 [532] 90 [562] 94 [472] 100 [495] 104 [652] 115 [591 906] 258 [493] 271 [536 418] 282 [536] 286 [503] 296 [316] 336 [474] 346 [536] 349 [245] 361 [255] 364 [479] 367 [245] 373 [813 536] 381 [537] 393 [536] 396 [355] 400 [398] 410 [346] 437 [536] 448 [473 745] 454 [459 473] 853 [257] 855 [275] 859 [257] 882 [306] 894 [311 311] 1004 [506 506 506 506] 1009 [506 506] 1012 [506 506]]>>
endobj
43 0 obj
<</Filter/FlateDecode/Length 608>>stream
Ú©DpYÚÐ{rð)Áý[þÔeÐÀÚ­ï]¼µo«È‡U*LÿtÄàyEu¸ÀO!x]Ž¯Lá
Š©È Õ±õ&
Ç*ài¾–&Å	èt4¼CÀQ›ñF„ÖÊ+z{¬	;Üœi 
lg¢UwÇg‡(
 ¡(-~¥ÃÁÔ 

¿vúâºaY•cmÂÖê}ûý@’?.ð-^”Ïxu«Ù0ÓÕÍž÷TS'ä½b&‚ò®ícVëŠÇIÂr“ìË
ÅýºÝÏ©æÙðÇH«²ÉäeXâ.Py¤ng_¿£¶ï;Ô‚'4é…TL©û«Gö‹®T½˜¯óPð01$[Ï@˜ÎABÃ%¹äýeƒlÍúäh=�êq¦9,(^ÇN|c[÷YóZJ›ò°À¬C…ÆíÍÜYt*€ÎÿEÆ˜ò¿p 1éž:œ·ÊÖ®º{ÉÁŸlÐÚLÆç&8OfÂîÙ´À"# •“Ä7ÃS×12¼ àèø|ð¨ÞWh=“¡õ‰¨ UÀâ^éUe½ËvÂRSèm«™~ýÃGŸvÕ
È~ïªµ”wzÞ‰$*…n-1Ng1.îI€šIŒ]b8¬£ŸŠÝ]>Þ§½õ/l¸bm£�Û$ºßlÃŽL*ëX·ýã¹KÿØmbYËŒ¸Bv×]SF7"
À&ì×¯P2ÞXçwR+®@ÆÄèÀëSÎ
WòÐœÌc-æ€«Åi/‹ä]ug"Iq‘°IWÈÁ–W¿8_eà×èµ3ƒz
öwÜ®juüµ³yÜ“™H~¥­hð
endstream
endobj
47 0 obj
<</BaseFont/HQXYDH+Calibri/CIDSystemInfo<</Ordering(Äûíî±\037òO‰²´T\023"R–Ø¡vÖ_³Ê\037§\020‚ã{3˜\027)/Registry(Äûíî±\037òO‰²´T\023"R–;]K™ä‚­ùô{¶ä›\035æÙ)/Supplement 0>>/CIDToGIDMap/Identity/DW 1000/FontDescriptor 44 0 R/Subtype/CIDFontType2/Type/Font/W[3 [226 578] 17 [543 533] 24 [615] 28 [488] 38 [459 630] 44 [623] 47 [251] 62 [420] 68 [854 645] 75 [662] 87 [516] 90 [542] 100 [487] 115 [567] 258 [479] 271 [525 422] 282 [525] 286 [497] 296 [305] 336 [470] 346 [525] 349 [229] 361 [239] 364 [454] 367 [229] 373 [798 525] 381 [527] 393 [525] 396 [348] 400 [391] 410 [334] 437 [525] 448 [451 714] 454 [433 452] 853 [249] 855 [267 252] 859 [249] 876 [386] 882 [306] 891 [498] 894 [303 303] 910 [498] 923 [894] 1004 [506 506 506 506 506 506 506] 1012 [506 506] 1081 [714]]>>
endobj
48 0 obj
<</Filter/FlateDecode/Length 640>>stream
·±€ý¿Ð3æ·YfÏDg‘]õáÕ*Î^ÛâayXÎÈ%KüW°8,!.î4ú™ôè´-˜ÈGèÖ×3-ãœß%)ò—fËZ-£Nœ
znÜ3h{T.1ÐÇ$D-»Þ«ŒLçI…‡ÌÃ1y´ëÐ§‚!óÊzõŸ”ÚyS<ƒ7+—á4Oäó;«†Yföå¤šž·.,—�’õ•ùbž×77[ˆÄ
0®V2-¨}BÆ5åT.…ƒÈZÙUŒÔ—qÀ·Æs³
JX |t&ZéR”vÚfÝ®G\Úóî£—òg‡pÊÓMQ5tþõìépÐ#{ÔI¿ùHLÖa·óÏx«n¿:_eôšW.€ÝÍ‘lõò®×’ÄÝ­×j¨Ö€.
Ýì¯Ò¨LH¬ê•Ì¶ù‰Ruó]"ud¢Á¬’ÕV˜‚£I
ml:9½÷2,K}ü÷‘ü÷¶×ï8ŽiÓ­LÿÍ
·3a5IÅ*ö·R<<»¯ï1ËWµ–ÿOa 2h:Cò(,,ofîk=Í¾Àg¦‘Gé¨é_ˆÉU2^óerš“n\]*¨BCÛˆÍ£DŠ·c[ÅÙ§ßGÈšø¶$ÛÄ~ôâÁ¢´°œ{«{™‹á‹XÌ£ãWÛH_®ìË]
35ÃÌD•¾fÜ°YyÐñJE¾È[Laá'ÛØË¢O'0K80‘ßàq ÔÌ:1C}•)U:*‚Œ|úº‹…â&Év”2ÝÉôl‚dT85¯gˆ‚ã=Æ…mo	á×�7˜BCRö””1`=·êŒ	á'Áî:ÛJ\ä_j]Žs^+
endstream
endobj
49 0 obj
<</BitsPerComponent 8/ColorSpace/DeviceGray/Filter/FlateDecode/Height 97/Length 1280/Subtype/Image/Type/XObject/Width 150>>stream
&‹wq‘Äb+òØ®ÜŸž~MþÅ7’X·ë"í…aÍ¾²Ž.€lbbË¡÷¸†û|¨J¤…]à3K·õ»(›JI¹U\½°¥šÐ¾ÌØ´ Ã	]òé‡Á@ãÕ€
ë¨1Ä²’œiŸŸR5RÌÛdä71ìeÆªÙ¡(ë‰~„›A*ån€O`%Üé’9˜Bê´�ÖJŒßoýŠ‰	xDzù$ó?™ª}=&škö ÈÄ±®‘®/ÄW><Ò†–±±S>‹€|dýãù©yÜÇU€‚íðZQQŸ
Ì´»èz[¢VÏƒÏÈl";¸4²-|sH-kßíÁÀR >_£V×9Ö²áè¤Wêí6‹JjÖTˆ¶¦Ð¸ûíxþBëw'Ç4çƒ:›]¸‹IiP°jÈ1uXâ„w{95¹�Š‡€<–lL
üÛÛ‘c…„ðâáýnm<þàË (x^  E¼Õ_þš4XÇvðZr ¾TYdYzr‹,~¹Ýù™W“˜vøº)QÕ¯®Ãs
Á¦1˜ˆº~ë#Ýî4þ°i`Åõ5[ú»²!Ågù£ùG6Ù0Þ
Âë|­:<­Öê·¶§&Üóxu%Ù4þqé{
e¯«`"ã�Aì?çÒôì4ÓÝçÔ"‡Ô=/é#sk˜.xÎ÷Š¬J!-M8á ™.F•" Xkw_3êÂÔÕ{.yö¬­Ô…€%‰xüÿjG~/Ñs:»­yÐÇ ÿ‡ÞUÃYÜ¤t¶€‹l*ÎËGªb”ˆüäÓ”É(þi^f’õZ÷ÇgÞô·Ý=UôÕZåwô£¾õÞïæv´ç

›ÐÍŒÔõ‰)”±
ÓX8¾6Inc÷ƒ8}/>‘ûêM{"y~Þ‹]ÆÙÁøÕ=îÍyË`Ù‚ÝÏÁˆUi É1…òÔd)›û‹ˆn—ÆïKšÐjÝ
Cs‡™ùFÑÇ†ÔÔjG±‘Às²ÑÝ'õÔ²FsfBèÑuœö¿%»“_¬ˆÕ*ÙS©’oÈ`®
=µñ’Æƒl,©dR×¶¸æ¹ÒRP¤÷Û7†—Ð 1’ŸøÐBÕmilýe¡!�Úá›Ïuj“„-Ýù¬ý–18ºˆfñÍQ€Ú£Ÿt›ß5eéßšÇéîP:„¶˜dw�Qå_›ïý>a¸-ß³�^Ìe÷u-¿«USW¡U,fÃæòwœd”C6–ÏßíÌgaSÜ2rÕîŽJ¿ÛåE—/:®g¿EéÍm³ÁSÃaNÈ7XÛÛÝijlnZÿ™f’¯0-À¾½Aµ[]uši/½Þx8íŠhUfp>aÇƒ0D¿µÒlª%rŽæÂ‚-Ó°ÓÁÚ½aµDÕº”A-g¹j'P áÉu:áö«IÒÑ<”¼lc™’Þe9kžÿ—&'˜ED˜çH{»|N>eõPî0"fX¥.r/MôÞ›ÔC¯ÿ®é.õEÍa¨ÙÑ\«ªÛ�%Í[;8ê|–ÒFƒ�A6RÎ{ÛÃ{Æ¸Úºø,¾)Ï$€ƒ‚W¡“'½k—:!gÛÅÏõ¯Ú£œUVÇ3[“P#kSžcŽ·ž˜Ê¹gb×°Wÿ_H¸è.ø—Œf¨³þ‰[Ý2±Äq@1®ŒÞ*t§
‘	zöŒdÒ
endstream
endobj
50 0 obj
<</BitsPerComponent 8/ColorSpace/DeviceGray/Filter/FlateDecode/Height 97/Length 1280/Subtype/Image/Type/XObject/Width 150>>stream
hÈÓ¾ë$Fõx–´<æôø˜IšÛy�(ã!iÅzÅ}ÜAO'©^y=iy¦ðÀšL]¥È/	 ‚ß?»ù·Çb¤8a"’0~Pºm?K-Ð^Bü.]Í½ö&“5”¢oØ‰Å´Í¿£stïÍ8ÐsµàÎáÜDÃÛ3Å!Ú	X:»âÂø€àãÞ%ÔEuH¥”´–´j­Hó²UL©ÚãoE.ÿÖEà$g,×a¾FYA«6æ÷ZsMéåz>B˜ìŽÜ^(„ÄÙŽJè¬~vË(…)Ý)¡ã,ÇXæ-hÇXUà‡yœš½Iªî¢ñeR:y~o:Ùâªar–yºäH(‹©Ã2GóÛkC}•’Ïær;°ÖºŽ4oàr’û4lÅ}Ï]½–}Äe?õYiuÛnˆ\¨†i l1)ìŠÝ¤Û|3‘UCÇ=¤
ï•'l=É©sËÊ7áž×"ÎOÞ!gE²I¸…£ëáð„šÊ”BToð~àÒ­Á+M&ËÕmêdkdŽK'!Ÿ¿rgiƒµ$±Ž‰swg¥.Ý&Ï^ìhxðD(¦Ò^3ñXLâæ-´
ú¥zòº÷( ´ÖÍ¹<psÑVšˆ]iš¬ìàdQ"æ¨§M5|:
›‹Æ‰ÚDy@÷whÄÔÅç¾}~¯F>T¿µWFÝ„¯©"É(ä™Šy›oUÇ›œ-Â÷Óü/öª*1Ð“øh˜Åàa‚b…Oá7Zü:8p<‡êTç¹©&óRŒ<ãD½”_¤ÊÁ" í;¡¨jÚµŸfJb¸óUæÛ§0
ZÎ‡9k+VÈÉûûx·†f"|ù
›…€øKteê–s¦Óî{rºôt>È¾—7
;´	n®åÓ=À&:¯yaæè£þ2·×«}ïôeS?÷“å
’ÍÐ–ïºìÏs:×<Òƒ!L(8ÿÍ'WÃ„ùÈþî&àa<¥ày³¢ž)€C—óÄÓ‚ã*tñÏWI$å°OŒ?¸oazëSE„K]7gÅh:3…~#ñ2p:øBÐÐKÿ�9
Þó<ËË~ÓõÀ®B•TigÆt›£LLC‡+Þ¢È-”K³£PƒÛ%¿ßPÏq™ø6¾Ñô+P¸„ûwxA”9‘JWº`§TûÕ\YR´ÊÜµ Íè±k¹¸â¹û[Þtð…öç×ŒyâjXÝ
]Öœ+Ò<GW½o#Gõi‰Jä¨­}êãäU¬f³d—©ªÆ“LÅ.‰ÖB\î÷ËåuYFo,ºßq.	*!GÈYÏO)p�l p]ÇÊë]â*_^bëªs‹·ì•_°Ž•×·Ü>pŒ¡?Á=b[ºFûŒ7+ºÒ Çõ0éï-05ËAjþÍîEÒtÂïW‹"	!X§ÑCIªÒb`Å’ Æ/Ø“f6íaEÂVœ{5:ñ!+ŸÏÒ¦´<sñ?^¶`HÝ¼ä`åÂ¦YÖÚ§¤TäÖAI8:u‚0°Z‹u2o®ÿð››Òi_MŠÕ“`^[¡Öú>Ÿƒõ<¨ªŸ¨æîñ³ž£ÖùõÉÇqWZa¬ÓÈ7¾•§ß¢ùà»ëm‡~<¤çÆ^;#ñ“r°Z�p½´q4Xw¹­<8Lm&\
endstream
endobj
39 0 obj
<</Ascent 750/CIDSet 41 0 R/CapHeight 631/Descent -250/Flags 262176/FontBBox[-518 -349 1262 1039]/FontFile2 40 0 R/FontName/TFPAIV+Calibri-Bold/ItalicAngle 0/StemV 80/Style<</Panose<ed63213dfdf09ca0d8cc54f85f7b41ac6bceb19355e392a0f40c0abb91ba1bc1>>>/Type/FontDescriptor>>
endobj
40 0 obj
<</Filter/FlateDecode/Length 32832/Length1 96600>>stream
íˆ@Ab°77~%‡¬ÅôÆŠÐˆ¿ùæÒGþñÜH¯ìt#ýïlˆØ_ážœýÊž4Ô¡\ÌG³bËN‰?˜tO¸lM$I;ß.¿Î—�t¹èå7³Í:Ñ¶¨‘.raìEóv²åû–qMN¨ãray ^ÎÔÒè-ï…›–Ð.#÷ÒÀ]}W™É}÷4>I+«ŸåÕo¤ÔÛù"¨Ç¶0ØÇ¥ú,Õ˜½¥®¯½Ý(È¨./]õn‚³qSX!™Æ)HþEõïl)ŽõØQo¼ýPHÇ9‘ÌQÌ€‡veH¶T/•|89Møg­Jßrl'žÕÈ#ÜãPàS2X•°Ä b´ûŒdÆÊì
€Pº©r„·'-Õ 0fäîfØÒŠFÊ´höv²¢Eí—C)°„øÂîG-JâtT2oñh
›PÅ­ulpnÌâ]²húkÉ;òçÝn5.¯^«�Ç/O¨@%	K«&ÂS ù½ÇXËõá•š\¨Ó-w�QyP·o_ÿ
3ÆÁ…YK´R°½^:Ñï*PÔ]Ñ„ÎeHÏ6#}K»Tå¢.úxˆvºÜ–È€$/^gq«±©)Ÿ¶à´mñXò¨Œ°›«	JÕL„N`­ÓgÄúO<À49ÓpMaA+þÔ8l¾ñä±w^ŸÁaÝÁP9*Ñ2ÆBà†]Ñ:í÷¼ÊsÅÌŒ±`GQ+"¼Ô€ÇGÚÀÙïÍuöÈw ï®UH1Ì¡½²)¤öÚ™ËÆZÆ†"æKvü[%ë†ìYF†3†D�àEÀ«íò0è®­ÛG¥…F4¶ÒXý<™¤é4_¨ë,»¯3[$˜ºÕÎ/ŒÊzÿ«…'–¬ÍúŸk¯¶Þˆ©BÃþ4#7�iÙ­Ù×=ÐJ~Eë!xGWd£Fj›½Œ5N!ÕËtBX¤™NV"G}
3)‰|þ­þØ¿«£Fß;Ð%‹¿†\ŠÖ%Îˆž<ÊzH/Åg/µo7SFÐmN½YpÓo³$Ý¼ø÷€fÙ?K
€(¡(1)>uì�uö[ìåŒ“ò„š—vÇ(M¦< p€K²5J,ïÎé#XÁÚÑ|žýá*£w£™Ö‘÷ëã)ÔP‹œÀ3¡ÑùÙ'E˜ V �…ô2€$œ¤‰Ú…$x?t“ÕK«[¼¿igÍ‰zoj+Ùm¥ü^‘*«}„4Žë/a\®S¯â€·›²–iD-XwÙºšéW©~.Ët’TäTÐÜÇ2¦¸Ê#q˜ÔÄØ¡˜&sÃáÉË¥ÈV/}†€�cÄø^…_³E¿¤ñ üÕ¨‰líñõ¢žŸëkˆMJ¬šOo’\9-3Ð§ê&Æ …‡él=Î«È ooÓ‚þüÔáíúÀ¤8¼¸ÒY$¢IØºëw[Ý2jíúž®¼Ï;‡—¯ëv:g|%œ¡¥˜ƒ`Œåˆîé´v,vƒ›Áž''YU)`µb–t�ÿÈÕUýXy¹È¿Ãfm¿‹ªC7×‚;ž·‹
•WLäéNûÌáK
/²ØÄûA) ;WRŠ¬JvtG`ÎYß/@U³?ÞhðÏ‡V$n£ãôÖ˜of0áŠí<áTñJ˜%Ž�>«Ä1ß•>±êðŒqSÉ”‘iLHŽR6öxµf_NÓ»ÌÖîÚDT£ø2mëÆMFyä¶V%˜ŸU@ÜÁ}n”ä“îÉGŸú¸Ñ4¥‰¯Î?'EÎ… ˆv–]j0™$DH\u”ˆøÞ|ò	zIÝVÍïî \3°ÆåôEâ/Úuae*ÕEPäc9bÑ+Ýtí%†_kÌ¿•¯|’Øícê& ¶ ¿ÞÕØ
ã¼©Äàd·ŠMÿèƒ49Àb}eC9Êûaç{ÿC÷h,^ˆ‰q³­¶Ó—LÄ>*qÙÁÙ;ÊÜÜÊ¯Njb2y¤âŒ.~¯8	Ø:ŸPÎàE^*û6ôjÿ^(äë°@vMf”.JôoC;Cñ\è«¬kK§c™õ`Ån'n™\²þÃWÙÆ/®[:´¦Ç$ú¢™&÷YkÓä©*fC™;z§šm;Iú¸w£W®}>£ñtóv2¢ÅS‘©ñ4#rƒï™IÎ�Î‘ÿCgÓ“Å*qä#Í;1ÄÎ=•Ð¤Š‰3iÅÔ\j¨,T7EêÌrNdõÖŒõXÍ–§¼5)¯Õ~ÑäŸ™IÚK—•Ê‚ûß—–ióÂS
?ÖOi-•µJ2ÛêX§‰Ò œõ¹$Â&ì·ôAÜ1ëGg`syòO›kËÉÀ7Ôà²–7ÞìMÍ¿ü„"‹È€À?Ûk/ÎtCLô(R³Ï:Ç¸D¥*žmnÿ˜:0{éé@£„Ï¨\}æäãìâ>ìl¸kñrWUì°<iÐCüµ¥“n²ÄÄ¯Æ-‰Q=Èp²‡lhx#àø¯sŠcóªŠçëIŽž9"“ß¹LU¾}®Åà©I§š
,¦Ýd°‘±§§¦”Uì¸´ƒ…‚…°7Ã±¶`‚!$µyHqd¦‘¯ÒýW&³MóôY³«à©>Øä/‡p]YL­ûøWW‰=4¥Œ9†©=ðí½	j¡Í¨¾ü“ô]ak`÷)û!öÓŸex¼®K¼15fÞL»ë7UºÍÏˆ[‹8<þ/+½MÉhˆs.}~¥†R{l$ì¯·žýï|ë¢¹Z�aèÆ±Øíd†¢bu+ÕÑPI“¤à3ŒÁÁAÍXD~Nj
*™fwgØ˜JŒfßî1±n•!&ì¸rûHšýW´e£*™|
™[b}+f8$ÚR“JÜ€`tƒ.	–^ÅŠ×åš–R£o®º:•d&\§‚
Ñte")ZK¬ÀÈÚÝºþø?>ÞÆÃ$&
fFS¥q;Êx‘~…"qÖ2î8Ä±B‡k
'Çïþ.p
ÙoÛ†Ï@ä7#t%+¬tÐhÚ±^õ‘*¯lXqÍ>C%Ô§a‘5dD
DO“`@t¤ñ2¤EìäÊ}¬…Q|áµÃúë49“ŠsÜÇ…Ødu+«—,}3ûch£9þV–ª’/L¾pëqë|ãlK|kÄå]™‰.*yTîV]Ã#K¼tï6]°ÐÉ "ió}®·¢sæ]-=<Ÿ2š_¡•w4+mÇãŠEëfëiR�óÐLßµ6E ƒÖ6ê§û8dŒ´‡T]žƒÝÝ0”¾I&Wv5#ï¼§@jl¥LzÒì¶ÆÃ–lÎ±@C¨Å·Åx?½6•ac
’ç…¼bâC‰ô/¹{è$|ŒÔ‰}ÍBÞÝ²
ÈÍ"Î¶k¶bÉP—§ó–¥#ªªE$èIxí‘¡†™‚€r"G@þüá¶Î…;ÞkHÉ‹Ï¤ûæ":ö×ÙC¿Y!@šuíùžÞÞªK^6eêÚßfÕlá+š‘Q9ž
\¬UõÜvkBïaü£}ïW×<ëN•
|ïC4ìz}›vÎç[ùëA*)U'	FÈ)üÙ@þÑ²*Á§PvÌ<ÕíÖçTÃ´"zO¿Ûj‡¹¸I3Xfê,ªz^dÈr`6ìE÷3¶ÉØš–âÜ+ö£èæ‚ú<YR€·à3 EÉ€ÛÓ­@ÐÅÈmãEºáPÌvÛgoåã¤¾dwr&4v»¨–'’Jô}x# \Œ“"X‰r¯ï¦JOœ`Î)lªÖ›Hèü¬uf	˜9²‰5¤RZÄo™£vÌe‘á¢è’ûÖic1LÞ…žzÄAJ¯yL¦ï_ðn-¹+qàçÝÒSIjæÀÛ ½„plÍ€&Ë,Ždä‹ì¶”Aü›m”‘?ƒG÷A7©àU°ñ7äØÚ«°‡Ú¾œæÍ °{Ã /8žÆÉ°ÃÉ€V¢œdÎ*Ï˜,K³t•ŸÉ6Æ+‰0Ñÿ#ýê®+ªÊòUX°’n-8Cí¶¸6&àºïî´»©}úÎáO“œ“§F‘öÀ3%h{Ëê.ãHå[t$ÅÏØÈº¿œña-<h1ƒq«ý¤÷2Øþ=³Š0N©O,;õ& ÏÁ]rkÀÇ2^0P@Ù:©?£zrŽ¯y[.‡Xî´í}°[»†è5<Î†B¬Ž¿ø›ýû;dFìËõsÝ‚to½Žˆë Ž€�UÉQíŠãïD¡Ãä-æ‚ëÿ_¥òÐ2r'PƒZû¨ùNÿ‘Ïš	?å»ƒ=4)¹Á‚.æ~z¦ûÙåÄžûí‡äñü3ÒsešX¯.a]VC`úÚG0ñs*> ~†qÙ/RérkÊ<âo‚õ9*››—ÕAXL›°Dkhy¬ëÿ›wå—DÚ»dF‰•¿×”ÿ®Ò¹Õ=!ïVÛQqâ¶(3¾ñgZ‹<žÃ$1ÔŠCÓCÂY3aÎaf]<]´kÞ0Ç÷Dù¯Ù*äNÊL¢ìR2ì3¤_¨]_”^¸–ýð±;T³5ÁC3|’áa?Yæqw32íHÞ/ær{Öf£ÂKÏâ=˜òrªë*€ß\ï#ðáZ:*…&šØ.þL.1ƒÈyÓô2€6»ºæ€"ÀrIÒ£!Â9òçÒÄ±}|W‹Žu”ÄAÍ<Ö€†Z¼³vSí‘µ'”S"¹: NY
gØêŸB-¸–ÜÏ.#¿ªð÷JýËoZçdpy5v"è6{<2¬óÈ²¶-‡gnTa¿–íG ÈáŠù˜Ñ£½êá²{÷ÔséÀwÖX³Ñ>SÑ²--Ùx®Ë™²s
ß­C*³A!7l‘€ùØÞEÆopüÖ?8®ç‹¡iŠ
¢Ö·R_¥<Q„”_ï%'\šüÊ“`eK	 'B—å¬qûøÛš¦„]Èƒ€ð¯ï'hëj^yë¾¬9×
ùžúLëUçC†8-’÷}Í4<‰8i[ZÛg_âevðJô.pž4Y°tózïøK9_–=ƒnôårø¥GE“é3ÛöQ2ûyq“Š?3Tž/Ô<‰'q¯åêI®€s†<¸· Ý7!ïe§ÐÊ@.<»‘Ö2ß«®$Âñg"Á–HÈPOkŽ|ýpã‹ø¾Ÿã-Óæªq=¶#|Z¹»MþÏQÏKÅÐÚuD˜]Ág‚¢EÿUEÛºªuI×ny²•ógýø1aRðÊË|Iiç˜JÃTè>zÉšÁt‹( '¬Oö×‡ƒ	Ûð7ˆ"rN»ÔÖ‰.kºÕÍE,D£Ù¹ø}1œ¤ÁC¬=i?²Å-¢Uô”+Ž(3•WaÀîOÏ%qÛœ÷ŒiÙWÝ,ùÚ
ñr
9BÄðÌ ókMB¥±¢bÕXöŒÖíMÊfñÊyå+á ÅIƒ*2ìë¾Ê(äÛr•
%¥Ô½¨][#ÜßûŽ^’Q[g½O]®ŸäúâÆò€«P&|ôÏº*˜p[hŽ~µqxZNƒÝ²{]éÆ*c"#'ßßÀ$CU^ˆ{(OŠAEÃFÆ5g~XW›Èu°(¤7ª%dÈg|È-Pz*vn¦Ñ.NÓïÊ0ÛH!Í�ÔdÌPn:9ïAÖF…t©a¦Q]<ºžˆçÕq¿ÜdvOjj.„TÆ<Aa:)&ñæ„±ŠÅÉšEíñô‡ç‘í—;üZ{ºõÊi‡…DY§…?éÝ'Í›FˆI¢NUÝ1Vùýðiuê57Ÿd²’9u(5ü‚øÙº‰ßÈŠy³	÷2êÞÅŒ}UKºQ`qvF@ï)04£ÅA<õZ+•¬\¡ D˜O
µçÑ€¤G\Ì{JÂµÊñ:°ŽG·$G”ÎÊ¾ÒŸòäŠá[Q,u*Óaw3S©|asGŸÇÿm6ôôQè6ÃøQ#`%uï6+m(=¤øº´8iÂ×
1Û¿pmŸ¥¼‚C¬€¸,Òùéç
ñRñH\§ÔKÿÂ_.2cõÞæ„à
SÎ|òÏÒdç¾ÒQš“™˜%š37BÜâÏ-1ôƒ}Tâ3D*.$ï…z²BPˆ¶áíy\‰ùÈ2
>rÇæ°›Kˆ1çS=]^J®pF4ò%fö3>’B±Þúy{Ì(¢}ÖyÉNMÒcoÜj‡öÑ+€p?9¤IÀþ¯Âªå¨õ¤¬A0ÊO>ï‚æûuƒ EµÂSøNåáRQ
·øJ|:3—Ã!–éHñˆ*8ùºoäÁæß´BNRçœÍ&2ù™oÉS­‘ôi”õßç´/ËWÒÇÜ©«
’ôø€€)4Uº¦‚s$³íÀ½ª]¾®õÔ[ý@#ßàü¢Kß–UízOOŸûIÇJ)âuðÆÖÌä&ªCÈÿžôt$Ly9šué1Š7wƒÃÕt?Òµ‘î�³«8¡ân‚Õ]´6˜äNDrÄg³µºËØý6iÑ•ÑÏ«Ôm?¥à…sñî¿v|>Ñ›t¦`£”«"ýDŒG]­”Ÿ%Ç»}(¿Ðöë°[t|x7Ð<®*¢ò7°ˆ EzÎ‹k/ƒOÃ@þ¼"êg„ãCÍôìvÞ@hå¨M¬Èsv°eC&OÀèÂi>g*«¤Ññ	óÞ©?‹
¾IñhôÝ·`óÓÝáÒ›×ïüùoäÕm|Òkk2G;7âÇì,.ÄëiÌ=±6t/»ƒZÎ7F‹+ÄO3fÅäåÏvE#÷Ó
ñ
[Js‹YÞ¶ñÓÑ/À¦ˆôrØ‚†ŠçÈF˜ï_*î›N°ytÄ¢#¬øCE„À“VËM¥Ëâwº†^6Â�@gÍUÉ!ÇÅ±2ºNŒ…oY¦Lt
Žó)"ó*#Ecüý7‹½‚Õ5«œŽàJ§ ÊŽz^\D=ÙÁûÎ;‰íq­OÆ«^jt¼Ç™¢
£“ù£Ì0ÊX[‘ogŽŽ‰ÜÈ Ì¾N8QI45l³F­ÌÒE±b¾øÕEÀ„nß¦7‘ÓfäºT81$Ãg�Ïh]KM†ž
ÿsÎ`~±‚°`åHõŸ]˜~·0ýe¾%D¥µÖÒd QQƒ}ug,Æ
·Û®UšwÈn^¢±Ã Ÿ‰Lq4ÒŒŸ=d0˜€þ¥==šÿvHú&{IK¹cäž1&æàÝ#Î­^áµ@‘ëäÖ£¾¥ÿü® LÅ5PæurEL§>,
ËøšLÕ»šõè_9~‘`'J,ÓùZwo{Õ}r$sE;£âÛsÂ½ÁA.´´\ëõe¸¶v âvo[kÌsÛÅ­î^‘)gSÈqo½`^þxè5;‹œõî½C6ï,‰ðñ¬±>UH¨šmª“|XSP5ZÃ¯vý3ãÞ÷0h¼N©ñ"Ô;1Á4È:ea³÷l�¡›j¬¦Ï«5$¥,ZÌz"AØ{×y*Í¹s¶==‘ûlOP!ß×Ê»{vY&÷\p/¯,ÈÏ;$cÝXö+¶?	Á»¿­<c$¦g’Qþòç^k=XX;prV¹Y?ßÍ@XËÕYúÄþñbKãn™-Oh0ÂÕh
ÿÇÈå!Hx¡¹+¦¿€9ÈKu	ÓMp+½-fAJ8î6öptÐÍâtJkS2ZÂ¿;ÜvO“Ñ‡Ó`ÙñÜtDû?iÒÃÅã+Lˆ/~w&é
s?Á;ì=ÓiD„¹ÐÉÛuhìÿóãÀg>MÀ[E‰@!­Oèã°mÁ¾KÜØ½±òÑR·ÝÿògN¿ó±Úlì•Æß*ý^[‡e”À¥˜sæî•	d½d§ua#ÌÜ¿—¦ír íC¼M!"š/uY?­?3ùŸ©£Ð\K¹%Ú°:@Æ=ÙIûQ¥8¹ZÉÂwoaÿ#zrM«¸ƒ˜"PâzT4ì»Ì8›qì–àêWZb"ÿ±¼oP}fXè|áBÐK•ª‚y1åLh$˜y	üŠYc^p	YÌÑ5bÒ2=Ú Ö2;·ôLóÇòG{*Bfà×c&ÀñD“Ž‹
ÆKÖù|“¢KÒ}#½<£3º´wnÔÛðÓ÷rù‘V@Ž
ô(Èù^Qg,¼`:-?E‡3—¦-ÎØ¾éÃ3æŽ¶D]'§ZÁÿ£5ð�XýRÙX{c‘aT0Ä¹Jm#rs¯M–4—gë°àU
�ˆ¯mPi”5WˆÞ°mD<ÇÝVVÿ9ï/ûK¯j#5�ÿÓ""‡M'²ùÿK°Z¯|zÀ[ÜÂ™°hlÖUC–L÷³“o0¥ò)=Õ›üÃÆž4ƒ%ì›`œ
¾
ÁH÷Å@z™‡éS¬þ2ÎÒ
Cvx™v„Nz~´CÂ7|"3ùÀ‰Ú„ºÊâM«ÿ”‹Ç!J/P:¤
®|ñ!­
Q.æÂîî‚6n¦]œ+yi®ŠÏàÇ°À´ž¨Úâ¯·Á]:(¯.„ò)£»¿ºù¬ó ª&öHÔÚšiôÒ=Ô»s½!gà={Á¥ö[s%„y)ÿR¼¾::4râ5·Ç<^\›ÏÄß?¤Ÿ0BHKD#Žä~[^Î°×‚‚ñf-sÕ<‚0à-€à ¨8ÒœÐÇÑ‚¤`“Ç÷nã÷«Ä?ŠÐf¿ù™Äç‹á¼ÁÞÐ+³»ôï¼&·¯ þ§®A~Œ§}ÛTzVÒ¥…¥%*è"¼°‚|SÞMx1•â¡]
±»nî?”8X‹+Ya «‰?°è9™ÈÜüà{9×à	fãU³ï•É³kåLQÆh1((ÃÒpSµq&N,h37ñ¸l
kÞéˆÕtÁ‚¿eÀo%¢jžÌñŠƒ@MW„Å¤¦Ø³²:Âd—†‡ÎÿcÙGø=>Fî!Ýsw—MCöOKàfs#)ñÐÈ³4îzLLD&”–ðv0¼f*Ø¿ßIKF~T›—Oµ½•}T¡Á3wÑ±ƒd›#y\ºÉp¸HêºcçSÃ‚§r «m.®wŸqÂAºŸ{PT;ÛT$ø·ýYß‡.RçFf4›g¢“\^$_nVŽà­^Þ3i5ž'*yzaèC§Ä'
=¯^Â¬ðÁšsWs§©×
ÅLpÛHÉ‘áïˆÔxU%Ñ:öqvRÁ,¿›³±$Kt¨OX!ý8¼€øÌÓ*±°!UÈÛ:Ë¿÷¯vZÏàÜ&˜³O=Êd% Ìl1îG3¾Û†~ŸÏ÷7’¦¡Ú#HÁx1è8@
	KÇ3ùêé¾l#Auæ~9©ä%¥Çi+M8C`5«hÜ-u’•°æÛ)Ü÷Pyê¿}Pð_µ‹õ‚â]–A>”y6?.ºò<%›A€vbgÌˆ#Ö¼¼[ŠŒÜæÜ}à"±½FCZu!+œìHKa„_é^¶¦®Á¦ôù.ñà=VÓšc(2ŠÈzš³_Âkþ-L`H8Œ†É´
oI"w:,ÜW
öç‡»yCÉþ›cÍ=²”ƒ-At€Ã ‚×SvP ç*!ó<•}¸CT+Õj…^Óœ™Ã'’…Áã'¾šD›Ÿk‘Èsž+X7u[–™Â´ãªÙµç«f+JºX;£†ÝÃB¤º#…äfîšMr7
#Åõ6‹eÝrÎä!·ÌClÓ¶ZÿKôžV»!]#ªæjH‹f[§Ü!é¾
ã¬_Bº*^9Ü‡P7ÂcÍBgç$7çStúr³
T¾HÛ±¬a/ñþÚ6ià©LsC@Kò.*]‰:‰·ôØx4Ö`ï´O}Sk}E•¿Vc6"Àá
Õ'•ô0;P½à£ÿ<ÉA•JÖœXWŠQÕ®ÜÚ¶k*ñfË*r¸
d»G¼^øMÇ(“:â a b!J4§—?döÜ¨ð¨P`'›©·àý Úá,öæ#r´%…tOÑãœã
¼tÔÊ9=’[t$¾÷‰.IL+’nûl°õ_~~
{ˆ=²mˆ7+Ûù¼Î<7Ëœ/~<?’KGÆƒÏxœaŒ!—Úé5­ƒýÞÛ$Õ–.£»’…„¨
àM‹dF÷€¶
\ßüñô"-¼ìX­|¬rå¦ôôaª…3ð÷X÷)w)ütªe,$.S~‰HmRDãËRÉÏ(·¿fU¼ÊÜŸŠF;h%mïÌ‚Ó>ÊéI`ÅZ©„¯+Ï ñBÝÞWå–õm_ã§àÑæ`äÍDº{“}¯.4Ý”,ü‰
Ö»¾"•*åAáÜÝ&BÞ!B2+K¤†êgŠ â¥©eØ^”pµEC–Ú=•{‘–ë¬ž`}è“’¥Üyõ¯G¦q†~ÒÞ
¢‹dLfŠUØ¸°eö!ÿq’{ÏÙò¢ˆ\|ÃÏßÝ¡"p·4ß7X¯Rûª&«¢`²µ8Šû…8›
ˆ]	"ïèt,X×‡ÅÇ�4–ÖšÔñ
–^€«·°?4—õ®rB]£­“¼@d“_Ô‰ƒÈðä]•Ìõï¸pù3ßA
˜R„ÐàÆ¢oYËmY€o3A+&)džÔ-¹xºÞ­i»ãÚØ†Ò&Â…Ž-œkåêØ¯’]?è¼)*[ª�@D-U“
è¿´‰kL„ßa··änoJ85g’üEÊŒ8Ü•ÄG"¬MÞÌ-NàÁ¿%+Ë¾ôNa6V-ð±üüXêÂ¶4o¤Ë ¹T·²üAþ¸U%É«¢;
(‹…
K`åb¤°ˆfì—¾q‘¯ºR�û¡“œN 	ƒÁ±m_i%K;¯ëRà?Ríz{hM_¢U`DëZ»öwÓ/'?Jè%Ÿ(Méý iÝF@ÏÃ&QÙ•·Ë»*@M»4Æs‡¡øqXŸ³æPºQgÇø¾.¯ÅkÄ}î¹¢Cëí4òÑMÁRñA¯ùë<–•Å=«Ÿ—;@'ó5'Æµî‘<�(OBH£¢/ŸûÈ_ç4µ
#°ÞèÇojà ­‹eœ“
s
ª5Í¢o;^Aø<=qi”’*„@˜~”
£¬ÂÿSR¤^ru¤Yy‘RØÈ€5)Ç—ãÂä°‰[Ó9,MÝÍ/™ßS0À@Â _¿äýK·¢ô6¾€ ‹ì‹ñoíû&¨Kµz²AŸQ	 öÜz½ž¼gXB›ã‹¦"Ë›1ˆßùw¡å@™þh{wTÂ§/¿Q-c,•‚ÝR=‚ŠòåWÊ_Îõòäô”@wŸ
vêç‡Û&ÇfÃRU5VwXC£ç{\i>=å„öÇÝ=1ãûÚÁŸ#‘Ifß7×à-:„×5:TPÐ&ý0d½ÂAûÚj/Q>Æ½¦‡
+ÄU)%r;Jp|¼Eý("‹ ~ÌÀ¤#- ù´½}·è'}]„ô¢Ì’lœÒñ•ó£­‚YäuãùŽççm!û¨‹³]3f{U*:ÇÂ¢7$êÇzl0`fVl–ÿÜaZÖ­/ÿäÝkÊÌå, Yô Ú¼4D^ðWŸÂ³õ}Ùs%
cbªwž4Ú«¶LNƒ¶›š{<»`k_£ÿ¨ÿ‹¾^|Yú P”]"wñØ‡*ŒòÜ¹Q¬0__ó·ö(ŸmVÛ2LÐ+8“-ÀÇçDéÜ"q0ãœÆuv,ÙfÂ?	”žVg.ö1ÅQ›!ÉÈéA­šTÕüM+cÕA‰0AôôƒˆêÍø$í",<k3ñ[¶aà1WS5*žðgCG¢Äµ±zÁ)É§—¬OS`)ö¦®¸£ï›<¼håŒ{N‚ÚÈIÚÞÀáÎô¿ÅÎS
á›\  NIŠ…hhlFâŸC/#Ð,ªw‘¢µ«®?Ò
(YÕ¤ðjì!ƒëuå
l)Ámr^}Sõð£P·[?·­qAýð>.ô{E
Ãº³©ÖÃ©ø-Ó‘ø­–¤w,îâ�‘;º¼<ËkPÀ’a2AïY••×7V¤ChúY�ÄGÏ¢;}¸8øÿ™,uÈ=”à*Q‰ê>AxO?ƒ…
¾>fqâ v›/O%ËjúJP`ë¢¦¹+âð°�Ó JMR{çßÕoòM¾®B¯»»h
ó:ý2¬P›²»DE´àiZ/•¾5‡Ê•²ÆÑ¤Bºa¥–CøuæVE[ÞjDšÁÆ1`›{Y`ÏfŠŸ†°*ŒÉKÏÈäd7=lz—iHXqãÌ%M?HÜ"´‚÷´TRçò4™ƒÌíUpy.±ãxO!OÕÛ�úúˆÊf<·—#Ô_<-â¢5øö,ùoj;>È –¡¨·æ
š@x.H’Öb&çÄ7˜Žhh«íÄ^_1Ð¢!‚°nãYû¸CL6åhBë¤B;ÛñZ»ˆKÈÍhK}¾Wq)¥ÿ¶#\|§	óˆÝÁÝ
‚YÚNéB#ÚÎ„q°x.ÛÍíœÞc¦Ù¿µþQÒ‹¶A›
 cßn)_ÒAhïTqÌÈ·í*.ÀI{A¼çýçh²…¦µ2´²FÔâÂ©$wšÆÔÖ±~:^Ìò³]×FÀï"½ärýª¸	XÚ§w{\zSÎ»t·<pEqß(*ß·Ñk$$¨@4–‘ô}^¥{Ee'P»¨~ß})ÀJNŒyC–/EÓý@ÑS6‡gœ;)Û/O>¤2ô“¨4 i§Çp´ÙÜþ)7¶9Žž—ÚT2*?ïP�e%Ï¹q/x¤ú¾y•
š‡Q}]f¾Î­'jfØI/aä„Ê¥)}HN,lð×²‹1*VS¡b·éöÚÔ—ø	…‘|G°ÎNZæQéÕ~›V¡=]UüÉò¤c.o¾r¢à„‡<”Ü"¤’yÛ›™aSÔf'µ•h^
Ñï.1¸~=mÂ-=>¬ÅŸ©˜¿³Côo-iö¥IÇ±ò&¨ø— €$]´Î.d¨sÐö¤QáÌKæÃQñùp¨ëw‘‡òÞ>¨Ú0èÆF†ÐT6Â¶Æ¼×éâÅ¹ëC^O«{Œ2ÊåQ€Ëé­¾çÂÀ«á(ª¬”¯
âave¸´þ‘u"¶TËÛˆ*™Uî¡‰=’çw6Øc^œÝÞU
ZÇ®fã‘§Ž\-µ_wÑµ=A¤}TNGû%L¼"òØOú+çñ, ·²ßAWàÊÞ‚
÷mò»yF�ÜÔ44ßòñuÓ[WJØ
j“j+¿Þ
G$­¾×xJî
ß|÷ý²ÑÀ?ƒ
ÕK
ç¶JF{Ga�!ÀA6|àÜÜö’ÞÛÂø4‰š†&vÌôèµ]ßµÓM�7ÃÅc0€ózÔ‘³˜—BL6o“ˆyý
öY
EkL0{nq!iìèŒX…fÇŠ˜ÉÚvý}× s‡�ù6¿ÕÒ¨`Ü¦iú1Ò~
3¼2&øöïùý’à
o^DÖnâÃ¤ÆwÃ“àFò(]¤&Áqsá¥Ž¡ìMšÃ½ÿj:–“�™!^W)ÉýÛãÓJÉz´pC³rü0ßàyn.M[ûƒùl“ç®>g¡O@m_–0k>È§HØ¡³ùˆß%÷hÄ;´ëM°âÙç>WýÁB‹_†ä}
ßÿýgêc}'E±B4ŠK0“ÿJ3D+LÊà!Wßè‚>9Ù…óû>µÅ:+Gû.ŸÁ
ÉJ‹ˆäÐÊ|Êˆ¸ÁFMGp³ÔäA5%:rýU÷sú~�â*`V@ü8*ë+ÁÔîš¢âëf‚WàìñàTyW.tz°[ŽÔn¦0ËÔì×¬¼Ï(ÉšWøØ½;0ÅyÛ3¶Èøt|•?i+Í-P~[JO¾J
œ©ëÌzDo2(“Ô7o§É»Ik<i"à ?'ËVM†(¦KYÇs†‰˜p¦³m÷$Û—Öû"£O¡LjëÙG“N‰²‚np¦f¬Åõ&O¿ƒÈÄ½õ„^]R0ýª¶ÁeëiOBd,‘;Ó	Î…×M]BXïÖâ»ùýDd"ïtO‹â´ü‚÷Èßˆç×aÅ_ü.®ÐX«m‘V&îl.âiVE„ïRîÌžŠl³
YúKe“²‚†K§¶lJA|BŽ†t<LÃ8Äz«A¾ÝI¡ŸGD'òº=7¤šõX†RJ‚ŽOè¥ò»ÃqEÎÛ14"åƒÈÓrÐ¢­jÂß×RCöÌønoÆQÖ†£šøsA*ÓõÜ1uð.fSQüÙ',2yöC&öeÇ|ÝÞŠ5 @½Ü6›øæÐ×gÓ¯|×èPh´ÃË«&ôdõ•¤#»v žÅº+ãB�{×TW/ïÀP‡±ç›zøùVÅnp)¿I®U’D	«¶TšË´›…ö/4wì¹z~šÃüää`¬ò+\¼”6º'HS4›ÖJ'Xú’øÿ_Nƒ�Þ(“6Ÿô¡QØ{.Ž+dFDµ^¼¸ºoç·,‚•ûW)]Î*ÈêQbÜ¿ßÕIk(s$oæüPõ
9o¯é{j¯\=Ùhà4BZ8W�âÊøU¸Ça5�}¿ HÁƒšsmÑG¤¯ª£’—DÙ~Ùq`.öÆf´À=qðÉ>—F	™€ÜOGš’J¤~ôÆ™Õ#æè…]ìTj,kþöî°•ŒåÿF‚ìù?‡gá«*í|wiª§¡š
ä\2-^©áš½ÛÉa¢µ0‰÷yÀi Ib:á$Œ&¿†öhÌ"þ•(ÇãÚ/ì%¶^×†¤Øýg(·¢€{wÄ@„SŽŠ½’Ç3AKÒív%ÈÅ
«Pu•¨:3Y1r´[/¦üçýü{NåòÆå»Ñ…µ‚ŠJk=Ûâ|ûì€ìùÝŸHp·7jÜajRÒíÜ)–ÓÇk¸ù“:ÃyÅÑÃ
|«§3)£-m	…»pÅ]Û,Zèû­ JçÀØ»BÆ£û{‘H»_t‡ÖÃç’'ƒZu½é_±ºy(5öœj+ ©½P`¨’ÃðDÉöjÞÎŸu…±PŸ¦¿ñ¡¨¯,ŸuÐâ¬Žë?ÒCX	¿õ“ÅÍG†Ö‰¡èd€ZÚC).@çð‹
r½»}jñŠêµÐ×.­‹ÅX&5³øïK-ü)Å®N–ÖžUo{ì;lçÊ-âii_'£ÕUÂâlu×{Â²³€#“'»´6Ü2íb0&ñ,heþ‚Úf9`v™ÖCâ>$Ê¯€ûhXzqþÇ}ýP2|š4_„ßn‘Aœ$aiÀçF9Œ[z·‘Rýüð0ô¥Œ¶‹˜¢r7áük\üü¾óÞ	ÄžÕ6¸½¡1Nî?cŸ
¤ÁÎ÷-ÈzŸíe<ó0f3'âðí¶Á÷²Çò§=Z|,ÿ@7WùŸ¦ÅYÛ²¸½É
<Þ»ÝL \c”È¡,,¢³•A’a³…*H){W+à#@Ž)Óž½úˆ²
LY•¤hÏä?£7qs$¤÷Ç1ü‡r•F9jØ$)ÃÞ
Ò‘Ÿ°~‹v*‚'µÇ®Ê±Q?¶½ãïÜa%pc(rœÿ	ùxÀªs/ˆ"®ìXQ	®!Õ(¾<iÏq6	ß9p¶2Ýõ‹1ëÕ­³°,g´Ã™ûñBß<'/3/+,ìÐú¨(Ï•…s¬ÔQ˜�.u?_
+¦š$ÝXQÊ¢ëÆ´�
µW-Þl¹g:~ù2´ä¤3°0z¢UƒÑh{(WzžgÏv Gì4®
ögjB‰ãµœÈë5&O–<mþ85]¡»¯Ðšæµò?SM¦wZ^fÕzü™g¢I�ôÓ,BOÃ Ï$Õ;œ	éOÀ´>ß?ï¤½¬Y¯Å%¹â'Æ±--ËPó—4ÝáÀ¦áôö\Z—‘t‚±«òµéEBßõT_
Ý½Ñ}°¸—,)Lv.šû9¹c4ÃQQzÌó¿œ&ñ•6¤§£"¿l-f)�†8PäUnÑ:ã”–ÐluqÁæ+iK½ä}ÜWÅî*ªoQI-qIN5CPß†a|=€m³nˆ†,zï™ñÐdß·Ëq”¦¥5Iä/3Ì1€ÝÏ?ëêQíüÓ8ž°þ4Žnáq˜!zwM(Œ´#}$åeù2­å¸|MôpÌ¾ò‹ÜÕ•
®`£Ñçµ»èð”)î[_üèF>ž‘é¯è,ûRwR`]ÇÁ€V´ž=“÷í“¹ó7§Y÷dîŸˆ–```25ð|>[1Dú:QéŽ’¶3ô—¨òeLY0…ÖVd ÊVl²Ì‘S‰˜ú../­«†÷«7 îï,äGVŠ¯;b>r…Ì4Õ­ÐŒî¼ö»#6{dƒ¾†µ-Ó”šyF¦ó{=Ga….t
+þÙc5e­~`\ÇÍÕùáŽzšSÑ‹‚þá6»~©ÜMlF§têP€ß¯§Â˜!ŠáOòî,x÷4µÿëe‹|ƒo;Òb—ÑçpµÝ´!šD‰™l9øC¨ùèÄRý{sÎwüd«ûñ^9áœŽ×ÍyTâA%zcÍR%ävG§#Èe“úfáÔ¥TNíîV½8M¥™»J¹Äiz¦Ç¦³ó23f÷¤“à¦³RëÕyªÿÑ²Œ+…,ðÃÔ5ƒIä´QˆnŒ	
—aG"€Â¡Š{Õ]	½j2‡,^ÏÁûžMVbùéÓ¤¸+‹î®Q&²mu™×QBøY \›$vÈ£ß-$†_Ã¸6È]çi¦-W8¶ÝëíÅw~ã\"Ø¢Ti_J¾T“š\œØ¡Ã±JQlÏ:JpõÑÁ~— �Çm'Éhž¡*N.o>¦ôù¹Ee¶¦£a0•ˆ¨¥c‚•¦ÆLeQ[?ßôÔä¥ukv?ö¢Yí„7¦jÿ—q£'k!—äûçå¸}•éÛˆY	#wˆòRö£=˜6pªï;×+¡Y²F­åQa¹ù#e"ñDqž>AI€B	!Ì¼gâ­ÁŽFÓõNw&TGõA
¢ÅüŠk%'mä¯pä<,	ÌÜl‹)Ê®›¡=ìžI«lkDÌÓs@ý&«I@¨ãÈ°±²ÉHt¿›9!R@eî\úˆz©§Oû½•7yQÙ¤1¸$½S¼þîfáäeáV°¯N¸ÿòVW $þ5_	Kºv2Îú‡ENÖKÏ@ñ-Ð	†Ä>FœÔFÑ•Fd‹’ iÆY÷bz4ù9jN…5)-Žýv[UlR.GJ`Â‚úÆÄLÖõwÛ<Ú
%Os¦ºq¶¶ÑéÜ“…žÞ=*±Q¢œ”Îo4µ¥<:Ìü=,{áÈÁ\PÆÁ"‡Ønò5[£Ä§ ¾òGÿÃÄdiõ	Ï¨ø7(}êç½Á˜V$§± Ôerë€%Õç
ÓNYÿ–=cºýùÛÜ’"„åà··Xi(|B|í˜Ù?Ñ•‚Æ)Sù§„µû›P’ó,¨y!Üêëí›:È hgxE÷iŒ×höY€Ñ¢K-79P×#kçNŒ-×ø?–¨B¶]4>{×^‡ÅØGj`tÇü ‰ÚÑÈ	.ü1hY\ ‹ƒ»Lú.ðã½-ÙKrÝ«eß’ã)½ì$‹´ì&I†C
Šx´áO5k©ÖäÛ>6ÿˆ!6Í=ÎÉ`R…V¬ÝÂ‘µ,D^dMØë_ÿ¯ðÈ¤}"ß×§˜fd-s¹ØQû“KÈ+âÁTcð²Qníœ‡Ši®qÈœ.åÃºÑz«l¾ù8„.XÛ1Ä˜Ç’:�)r;ë¼×âxÄó­ø”—ËØX¬d}ÔómÃå+×O2`3Ï;®¥¨7YË¥Z{ò%žlIVu9˜Ùþ´5!ïêï8¥%Â°Q>ƒÊÃ).
óÂKË†ÿö°Â˜AëÚ·A~.àèÚ$ÚÍ_ÍŸtÄr7_ÄY#0•«w©•?×ì“"‘Â„¦L&€È*$¢£TíQ#< e¤:ÎÒ³ºç|¶7‚*>rÍ<Ð==­,%ÆÑ¼�ß›>…¸E¦Q‰6üðù£L¿üQBoˆÀ¼ UÚÆ!
!úNÃò€¦¤”F˜{³ó´´ßÓZïÊJ#ÀÒVSö4/q/û•É:ž¼Úû˜ÒG$1ÿü ÿ—±EÑñ:èo¼˜™|hŠd ˜Ea- â«Ct·ª$>ð½^C€ìÈ¶°˜jäXx•%NpJ^ûå«†9!&@ãvßC{OÐÞn›
Ôë+à¿-Ë¤{¯´
3p¶ySrÃžð¥§3MŸ$9ðçkÿf»K+HË'
¤ò3òo½1!Rýø:@í¸pàüò•µŠ-L\Ôj µ—|z÷Š£¼k
ÕdÔ ú]˜2ï(Á$'ˆ(È“üñ
æCŸb0ìIÞ«u¸Jm"Ê½&ü¤µåcˆÐræ-äk6>Ó-Q[çß~K%ü#uQaÂP
@weˆ9îkDˆÜ—ã‰Öôö‰dÚqd	DT1Ú^boqùñïñ”Bw„G$ºÉG‹ÓÛµ•¤…¸ñþ+ÊÂYkÔ+ë¶
Î…ÉVç¶ÂûivY´s?Ã†’ÅH†Æ¬Ž±¸<K2<Œáµ˜sßDÃÇ«¯§¡¸«óÎ
GR¶E9¤Ö/éuÃàgdzúG€Y?JÔ¥©œµòÑfž•î§1eïomoù*PI+tÀlR§«Ü¥s]À‚7Dµ%#õÄþ¯FŠ!-5ñ�¥‰áœ;åòåaô
åö‘OQ1äê¤†hºÊñì~‡æ#ÈX4YÄœ†.»zb×Ân¼kÙÅ-'¹*ý{gÕñÞòÈA§sóóªÆóv*wÔëaç0‚GÈŽÂþÅhY‹T*‚âm™POºn€øÒ« ]püÐä ÃO{×9—IÄ“]7=þ˜*Êðã’ª‰)¤³Ï¹'Iè!\Y–hi‰à[­$tµb}Ñ@žÈÎ{á‘‚ã©e²i¸3(”µ÷À5çux—S;Ì¾ÆV›|eÎÍjÈ`YÇ
ÊPÍÉ¤aH.‚ù¨X¥œ%WÉ?×Èã‹ä¥¾êÓï-Ðl$y¸xUÈ‡0ôÖŽd –ºG°b#Sq@Gdht¾k’hç™kŽ‡ê“Úß)oÛ†UÎøLzý„ÀMW’ËBñ‚¹m[´;˜îÆÑë�´¸“V0Ê­ÉìFù¿OÇé_#lýÖG¢ë½$ªk€»­Xo¶#
uÚHÞùÙgú¤2’3/YZ:D€µaîéqôÍÛ(ö§1Cp4“‚—:‹{ƒ@æÅÉ3V3%ÍÃŽ!õ¸m~ØÿÞÂttñÖEô‹KJÛÿ´Ü@ž¢Ì"Ø‡\/òTµÙyw‰×˜ØP¯{bÖß‘CÎ!i?‘SêJ¿»¾,ûUcJ|›¼Ì
]®¥îý©ÏáÚP·»w~ÑÙEx:`èëÞä„‚%»¾n³˜'ÐLZv)h*Ônë¦tyz£",„Ã—i_hWÊŸÖ¹˜Ë'\i@"b	¢C!¢ÀrÂ1ÙøÅÉe–uÍÐÛé-.®ª-¡ë}é£ðÆÍ}T”( p¦:N=€7ø’yS
ë±î=3<’7v>ÕÒI[Ý‘BûNüùgžŽæ¨O¹…à«QÕua]:Â­R¡,X¶§Ë°’!úÍÿƒŽšºŸËñEqyJ6ÊIÏa
>ÈA•3
z“ÍnÑFÑ‡TZ÷Ög(Œk7T¿%r]¨ÊÝaÙ­ˆSº‰2Ä<î¬ÝàsÆìtèe*R¼ØæN‡NôÏ&•‹ûÏß®¦OdŽa6é¯+±Ÿ¼XAj'™ìÆgxÿ~¢h2RjþnQéæý%~1™'‰ùÌ–ÒW˜ÚãVôš Ù6S\½±ª^#Ù/o‚Š£E,­å	k%M
˜/ßô„(_È¢òhÈ¨Ù©#öÂ½'F½4êh~{7æxI˜ûÀ&<'"|‡z
hØ{ŒáÂ¿[–>gÉ©l¾NB>¶H¨õ:gUX%Öÿ£5¬¡©	òØ¿‚Ò5p0ûWßå…ðÜ¦I„|«ìµ”T‡×�¡èQ¦&Õ)ÿlLPev,Pè¤
ŒÐÔÝ²ØúÙuLÏ-/D²”yúOÑ¥$Šj¿\,ïÎCn~nñë=¨‡
µî…öXÂø­�¿MÝýú1¬æl)¿!±˜#o\×¦è@u¿¡¨hé¢EÑ9‹‰ãôyÎ›"'g.V{‡.¼­bqü°fcÄmÜ¢§“&?Ú©z‘ïºæ\íÅ‡9ÀKI2OüÜ-Ê°~\)þÑ'IcŸ˜@ÿ~SíF;¬ßp²òì¯§Û6n9¸ÑîâR–a"ÄzóKuÖö«Ô¦.'âç,(®V
é+ªý}`æÆø·êã$ëy£AHÌ<ƒõàÈLUBû9”y‰?q"š 6>Û7´®¶ÁÄ´Qðçišo£]0tñ;^òPw÷fô&×\"Èj�ÖŸ=Š U?ªMÐb×åü½Ž ý$k™¥^@ûÞ³Y"á¶d9_rfZýgCS}r?#Æ/p‚F«žð”˜Ã.9ºÚJ3yâ)] Hµ,1BpG‘$3ml•SâhÃ¿ð±»;%þ»m'ÐõN�ŸAx\Bä:ŠN®ƒ\ÍSÞu‚æÌŽ;Ci1GÇÍÔIÍJöÅ	OÏ’=/(AŽ¶mqV:Êˆákx33âWóŒuênkÕÝ8kÙJUl~b‰Oá¢ƒùôß²ê…Ç¹Ë‡0h¤+üÐpÖÑ»¬Ê’iVyëou£œÙQ91R„H™ÞBàé7¡ü0š™×H=uU¹šã’’Í÷%znÅ}€á‰FÅÅ?* \pÚèÃ¹8;ï±wöVöR~ãpÔ‚éH=Ó¦·¡ATgÇÍ�˜÷Á'„(1çLø-âÙ8Ã»T¾O·àtÇù²´ÏI³'mûRH4êÔÑ)j÷	êý÷é1»Õ" ¨>|»´8%lzSÿ4Ù‘4q±+r8X,w·ú~ÕM3ð·OÆ¼%”N@ÛîNþ‘-þ(!Y†o"”T|äü¾C±$Ú{úCò† Ûò»h1Ô„Ä¬X+âƒ±³õmeç»U‰B!$HÞ0_Wã5³Ôz¬0Þoð^	y'¬ë5Jìš	<¼ˆp2yÊÐ'¢gFDö8ÐuÙwþ_„
[rË­R…xT¦‡Ñh›ßt‡9îª5T²?Õ�Ü,c[/c]Ö;JL§ÍO)¹²åÐ
Ónîé9"®f:vÛ¬hEo(±ÍÕ#UÑÜ\É=ùÌÿÂ3×ß›ëm£¢“õcÅôÌØnÑfñ�ÛFi”%åjÊTœYWïæ†
‚p°¹yM)HôÔRS±!DÔÃºQ›\ê£Ì]K²
õ¸æÈÛ¯~T|¢¡¼%W ÄQX‰Ú_¢[ÌF”†Élp³úÄîÿ”ëŒ{J²â½Nj7éyl¤Žw66¾º:œ7þ�½›’œ\o‚TFGëpGu€ÚTˆÆËÜ%¯!÷K!Wy6%”Ã)4š‡U8OXÊÞCÙfÈâ|s[´‡®6˜Ü²×DƒXßq7ñþBò 
wy Ûh
ÉÆ„ÔÛBÓÜ w…¿ßõäè«sŸÒ`ôå×jÙíÀY[s~Ç&µ3óãJ‘ÒÇØ_
ÙÈfBõý;×Ë�û	¾ëRy‘ïGÈ«Î}xø[»“ž=nÍÐŠ}ºq53rÿÎKË ¦KGËœã‘}Æƒ¬]	Æ09rí¹¥ c£IxvoS1 Ûp›ýXD÷/õ”°%Oäwp×ÙõNàðà«ÍùÑÄ^÷d«_ÿç[C–íG;Ú¨ÚÜ¯yŒš=X?í3·WU»B'Ø:G½–aO_
•€\4Âï_»»ÿâEÀ‹ŽéÏnÍáGe„
Ô_†ÑÞ{GTÜþ¾õÄ÷·ÈÑàmibPP
~6rÝU(p„hÃ(¾0ÄHôêÚJÞ8£Q6÷ž(<èF´ƒŸÄ÷Ñ±¯¡1^°üÌSu­%)NTcF_ÂXp`Ë‰”6Ñc¬¡À÷*&¹öùwäþ~oîÆ<9‰›òÂ	&ªZPë­X$°®ÇvŒ;w_ñ—B•Ï2±ˆWzØ„×N?y—˜$x!úÔc[¥¤ñ’ž¡Ú7Û(M‚×ëÔ_( nÄa½9¶Ày#eypÌÊÒt˜lë·�šGÄƒ¹mÓÇÃÊjÈÓb^?àP†àcõ³égŠ¨uÇ@ª5b1
dd5ó^ÏÁ"ÌœU8YöR{˜3cvsvÎ¯	Q}Œ3RrÜQÃ¥ðB¾`ÛíºEvaƒX
$§øóg°‡ËÞÿÙe]}^6N­…Ì_±•mÜÚ7ôOêMK^pÇþ:u
J÷ì“>ïVÚ5Á‘æ«Ÿ@IëIÖä>:œC5ñ*‹§ë)ÿ×œ�‡"ó‰YyGc(J[÷ù½F7tÀUÀ”—a¼áû!aÙº¶È¢Øÿ­Àç^¢¿WüÀhÜàÆG$a—Ø1ÓP³‡‰´ì’Û}½u‚Càûã
€"Wážd%ÍYOé6Çö¡ŽI_(×M¿\ï±íëaæW™7—ÅJ…ß;ÌµŒlë¿Ë2Í[’–DÃ7_çA¶–¥ŽKûŠîšxXoV¨ÃÃš¬'/y2n¢
Ptôa¸t­Ã5Ñ6´>ØX>Û-¡5ÎÉ8‹«¥¿finÏûø½œµc¹”ºíÆÛì*™$r
½U¡÷pöóÌþéV’;“@øqG/} Ô.nW_BK‰Äëêbm'Î7·×Bi3JœÐ÷.›oÁ±GòG%Tjû{”‚1D›
…¡`(‰ªizÿºy&MöÛ\-„Q®áÍ?ù!5t®¯5U²ÅÜ¦Í†S¡©ÑR`Œ;3ÅUdS
è’#�“w¥ôx4‘ËõBö%Y!ÁvkÁ»çj—XÞã€¡OÚˆlôe.Éá„c)ÂYH8@Ž·ùž¤…»ë#!¦Í8œµÓ:ùÉliÔf—³LÙºX=ÚÁqB‡DÐ®¶]ÝRÀÞLœÃÏ	˜
&;×ÁŒ¼iïçªï•.º¾oƒã#Œ³°ælJ&»­D]‚!z³z5PµÛ~Qîú¦ú@¾G@VÔÅ¢ñ“½cû
‹¾±|ø3¶êgÌ¤2òÒLúúÆøÝoáÝc5¶UÇRI­LÁ`Ô]	ýŸ,å‚eˆ)MÒË~¿�¡ÆbQsæê’@ß»ßMì0'Ë[N—BÈ5á˜EÅéeG¨…Ž9íƒS` ¤ÔLÒÐb#"åjÕ’ªeõVÓœpu=:\Ÿ~|fƒò:øŒÕVfÕ&¦¨øµb¢Ònº‡.Šv#ø—eBº¦¶aâºòôÑµ%#dÀlž¬ªÖaÍºtýFÙ`lñ@	qC»œ¶`:kÙoH™cÓä£šôêeB1q„˜Úü«¿d…kÿBcvWZÊP²híÏ»bÔ~éßO?„ðÜµB=~;*&
C†+rÙ\è(:y ¦ÛU¸oÓæ�|^aÒbiê‚€è«z[Žt ú(nf’Œ—ŽüÁs<äÿßrÝ“Ûõ6r™ oø« ƒßã_;¬
øH—»ÐbÅ´Í™g£¯û¯ÿÊŒ:ãO£Oè"1ÏôjuŠÀøwÝAÁÜè¸aëPYoŸÈŸ>n†j;\Ñ¾pÖÑ«
H<4à{¼-ÕâkýDŒ´Ø%gßùf+‹9�ÌfP£Ümçê”pXÑ¶m–>Ú,Y¸;×ñ+’Ý{\
•vË”edSð%yS_>n”‹ÜF®Õ{ Ð‡LHhVÛoû›dLA§4µ^)G!j)÷/O]8aýÅhzzwRÉÏ{Y· U&%]sæG]<K	š¤5¸�MÏ+Üs&ífùŽá¢rX
ÊØ*Vt[ ]Ë.
ÝKD]‹äÚ®è˜*3‰yëÒß«q>	“è.äÓÔ5þÆ9äŠ$h:Öš(þ
¹äþûNªÎp-¤lÈ}mýÇm	Á˜=/…7úéo‚˜YÖ¡ÈzßXÌËŸü˜ë(ˆ–mBDi ÷ ùfRÙõÄ§Š²?•¡”-*ôçŸîy
¶¡û~°v,ˆ¥)ìV`lGÌºEe%íÁN†è’ÅJÏm{eÁ_ÜÝŸ¿Žõ7+ž¿ÕìÕ#º‡—„pˆƒˆ†ë½':àYž§±µÄÞžéÛ³;ÓÆf<o&#A¹4LèlÇZ7¸Ó]?ö”Åú­¦)z¹5Æ¸õk|¦¶BI³ ŠHh'awÆeAtS€Jëiï…r½à{ä® >¦Es¦ý´ÁæîvZÙow´BF©V.�Y…éÊeïQü#æGÃ&ÍßbŸ×Voè	÷šBá<8a¯RvòAŠX–I©ÝFùñ’‡ËcX8×ÁÎP	¨	ÿaXÜ±¦38¥ƒ8Ÿ*|/«ŠXfß7ZG=ñ§ÄP¡øò-:[ýèøõ«Z¿:â*ÁoPt"êîfÉÁh£ã¼!ŽÉhÿ~D÷eökËà#EGóAüM|•J–’öÕŸ{=G.µî¬ßém°k7ðÚã’Y”& Øa&ö¹Ó¥§‚’Éð“5ÊòèÒ¡üÙò¥:~¬7eC;Êè4fµ-¼Å©:»ç˜/î{lô™*Â-Î–¹5$ªÕûªz{LUQO?PWh¯»Jw	‰¿’Àbû <Ø˜‰†ÂúáŸÚ¡•¢1]óíîcÒ¹!27+ÄÉÄ½u/Ì×Ð÷½jÎæŠ$`#è¦Ä‰A”i·OAkòÉšAŠÜNsPöt«!îüjj‘
Ãn¦MšÙ[½ºîÁ€ÎK<CÐ-eP€©ªÐ AÎ»lOIr4k±‘¼wÙ6(þ—\94…×°žÐ¥‹¡ômç#‰ÖqÂÝ+ë^fœ5•,+QÓ�r¡8‰9yQNvÅ*‘„ŸBÿ.Õkª!*?Qê@AÑÓ¶ÿ·›
ª•4#vT“§â†Å þžÊo­´r˜·šE®x%RU&ÑåÎ(‘®~eä•QÎÊÂó÷xpŸ¬Îäv‡RëBÂDßlo/¸[‹È“ôw'8gUr;�÷`8ŽLþx8_W¡Œùt§œç.ª¶ÜCxÛÈèþkvÑ¼—E.átXH='Ú˜†Of¢Ã‡B_Èù‡äýýÜø*Â|±Oø«>[OK6=ˆK"ÈcØäs=2!7ë4×Àøáí)\K.¯tVp€ Ë=át•¥TúÜ"ü‹¸ƒþj¡L¾,ØÔ›ƒ×uà½mÏaÉ‘#ßn
Ñ›JkAäõQMÓ»Ïlw’m®ÝÍ‚çÍç#~g_Ü²ZÿµÜm ³>^t+2h­0d¡O2Õo„§O¯ÃržÓ’ÆKé÷?ðâª21n&âl�egÅÿÍç™dÂæÜ4Í¤ÀTÏ‡ÄH$~´þº_Ðjaºßqf'èØ¯ò¸¥Ùb+Ék¯·àÂîz(d&âHl/—*HÜúæýâÍÞ44zE=cÖ»¤Á=¨rþg¿E¯KAçv¢3�‚Yû,—2(´ âÉûy±.6Ã7JG®Ý€ÛÓ(ó>¨w0BÇoÙä‡éd^æ…Öì‰û˜lëV71FÇ#R‘-zh”FäoíBx £aWéGªkÛ\]«úX›Š;8³L³ñºòõ˜Î‰�€ã:ýiû\h!½â¤cµ»4^:—ÈS1cŒ@zïM—öõ$L+ªj³xãQfƒÈ×„ÚÒs;öæ®]K{Ñ#²–ÀS¸´I@”+Yá.!·ïÆÞr‹8„"›ÍîãçËÊrÅ{ð’!	RÁ÷Õ«NœLªýdBr|	YP$íÀ
;ÁÖmrüeé‹W‘¶ù	‡Q}§#½ }Uê$‰e
<Ç=DÝŠˆÖ*‡—ÂBjI•tgAD-TåŒ‡ì×'·Õäô-$ˆX|…ÉÀw8ìê›¯ôIøÉÑ1"¬9CÚøFøƒÁ³õ²Ù${B‚®–í%W›·Ãmé•ŽRsúñÁ!ÖÈ{ÊË€R>íØÇy;T¯Ï µ@ˆ>å®óÃ¥„^Ýã%—‘öwÁ¥ZüAÐdÆÛ&A+£ï”®s)À&2sb…Lô–¬„R…‰2ˆZÂV,·Ñ`Fæ‡D“¦†õP	xÂ`ôø{“voüýÈª_ñ£Ã6=,üíqþˆ·=5Â}j™†ÚêÇS"~…ÔE¸aTøx@£8Dçd>&[U)) �&
s¯)ßsÖ†™‘ºSR_ÔÄš¯´OkŸ`ÔoÎÂAIC€ÉŒjq•­Ëx])ÂfæÅñLªÀÁýˆƒ÷É»‚EØ¶ÅqOX´úÇ�i; zzM…ÜºH}Ÿtë	Ã‹Kuà†Afƒ¯rP}ðJ¢9l+ö<kÈµýå5upÈý‚²Æ_Ÿm×I	ÛJTÀÅ¢qmêg<Ãú#á9Þ»—õ_›\?êLÿŽ÷í°g&6üÂ"Úu™¾&¤Ü)¿ÄTeZw4¨1é˜#/|¥X&#ÚßTw
vÉ~TÙ¬ž¶WAÇYoYµÿ	ˆNçä¡Ó0¦ñ­ì[õû,ŠZdÉñ'³oíëÿúí;«§1µ`aù[-3%¥%´zÙñoÔ—´­\|úi§!½Ø50ûÇ±ÁdQV±:í©v"ÒÃœíæÚÍñÜ%$à
ÖÏÿ@òü6Á~Ü ,x&‰“YR[�h{ÂŸ†ÙÆ¾p|Ä
×|Ã\ª½Yäÿžé’½ÇP¬Óˆ-ÀÜe.N}5Î‚^iV¿ÞE³5ò/EÙ`Æ‰Îð(¢¥îu¥Èy“¬Û¡A„3án ˆUa[ü<Þ"¿ïÃ iv(ªWeßz™[<t…]È%Qk5f×c={p9>z­„6žS½µ4‹<|RÒAp~Ñ|[Ñ¼ÍZ \[µIfEYó@L¬n�ÂÜËð·B(õáÑÖ”[xîW‡¨)Jï	¶‹â{2IÖ¡w<.ð‡g»˜Ó‰2;¼¦	ÛvP$+Ö%h�õ‡­ïs±ÕÔŸìFFp¡p«Å­ž‘™Ôœ¸·0·“:æøP‚SJ˜0Ý¿®cÍ5C~†‚Ë8¯Õf>lÂöB#Qî;½$9*õ{åtî¾ÐkdâÓîí¸ÜgUljEÖ”­`ËÇàE®M‚ÙsU	ÖoØ4Š±:7$bÞú>¥Þh~8þjûÍw\æâTª®ž]’z4ã"§•:çÛj’kžÐÁÒ”o7.Å°ÂMÐ:
þ„gº´/´$LØ*œÀ]ÐÄQ°¬×¡[†h=•-8·êð7ˆé1àÇ_T`È
ÿØ&›?Ý‘ç€8ÛªîTJ ÷§õçË½¬&%ÜþUÂâ÷D PÓ{•R®íñ
®û`”ù?¶ãj÷òÁî¿Iù!VîD¤ÊÔR¸]îgl‘V¾©øMFõSºÜ ø]µÓ[Ú—A86#:BMš@91¬’æ®xå'ïàÙ*(…bÒ}®	-·€þäe6M¥\<n¤½Ä’ÐÛJ°!ºH_MÏÍöu¸ý2€+¤†œî1ØvÆþ¡sz£lmÂPÜŒ×¹––¸‘žbûo”ÐÔ{¥¦åWGÖ#ÇÚ[	ki5¹x#½ÂM‹|}Ã¡d×nÆ»í4T¥E¶¦MNùÜéPJ€¶,V#:“’Û‰Ãä:èH™£;à· À±{!ø”	ì>‡le/¥œ1O š
¥®»–Ú
!Ê8×Db'iô½ MöwS_¹Åkñ^ýÌÈÂWä—bæŒÒ|¯Ÿõ$åå‘m:‚¶ëJfÑjâÝ²—ZYw%ÄØd|ˆZ¢Œâ#ž:pd	–nžoãë†‘ñGà_3	N,|sƒÅ&‹†sÖ‘‘b‡cÙŽ{aœ;Aˆ“!ûh.<+|æd“ÛÚ0æÛb†y®Þ!6j‘94ù·Ø¹)5ÀEÍIwäÄ¨À04ÇÓ¾�¡Èn&ƒ÷qF5É,·Ö)n\Go¢œ`Ëœ	©jˆQ¤áJü©²}�»èK9X†tˆ› ]gØU‚‰ùœû@—€˜F_>R„XT@Bsz;_¿®Ž¦_Âš¤’q
XŸ]¤8M§PŒF>üL»$Àf,…vr>DìBE²+8ç/‚„ûÆ´—>«´Pe¡‚ƒRã™¨´ÿ” 
NÒc?a-
F¶´ÁµÛíœA×*éé©PµT±!7¼°_>!b¥:slsÈ‰4Œ™•lZ€ÏUB:_?¦m#Ý45 òiœ^@]Q(>ß†,Ò®íTKÏû�ØÎa×ubv€ã>¼xˆg;wBÂtuÊà
‚Âµ³M}2·O7PjÐï1º™yõØMTÙÃËkþŠpÁYÄB¶È Þòk>$É›Ý¬wb>I1˜#7¦Û}09ÒeL>Ð<VY6=Þ=±ç{z�š+¹RëLšsd‰"Ö›g.Zþ
LÁÝq45¿ûÿ=’Ñ…¿€‹ê‰ÿ)ööŠƒ«Á‘Œ�7xHdCº’µÖöW+¢èÜMxæüe
äÉQFš®i ŸÏs²PXðpe…&‡¢Zò¬çöU<“ä€ŽcHÆŽC˜TöÞyáÇIW¦ØïÜçyZäLÐÍ*‚À×{` žˆ	wäugGmB9º/ÖQ½P4Ó™kÙ=HK#àíˆ†¬(˜Žmÿ=3õØî(ç‰MmÔRŠ‘Éó
W}÷|Ä™	Ú¾ÚUuRÆªyóö{CvË(<pi"È™UœèÕ­ùy_ËÌ#ùÌ›.0PEß× ²]e.aàe³Q7§6[ò‚—šØÊ¾þl3it“ÍÅRÉØ/ªöÒŠä¬M$Ä‹—;^èSïQ3q’ÕrIÆÏ?¦°”Ûõr,u+X\t¼þ¥RU(Ò
‰#Ydl‹ü{týáF¶É_ï¡íý!k0Ç¨Žæ´.q°½nð‘Šäv@&p0¿Çù}&U,„27úYß{/ÑD¤wÁ�˜TD¶Ùm0ËFÆð(ð´2
*PÅ=P™h3ÓlÍdIñˆ3N„1Õ(Bãð;Ú—?HVgú¼à>¶ÿÓSwU9%<Ñˆ’ÇœÕsÐET9f7Ïø©1Ô<2;<Áë[â~w&E·¬ç>A×ßÐ	„Ø8,ªlA—É3
ò^ì>ÏZóŽ×R÷t>Ïõßú}&j¬Ób[A~ujqÝ@séŠóì†Ð‹½{6å·õìŽÞn¿	ª‚iÓ©˜M¥ÞkÞ“7Qœ’«¨ÐV¢Pý›l¸ìÜi›uQ/À—ÜH8¡Õ2­¨ö‰ÜStÝ³¸¹PÅ@¼H�¬3ƒÏ®‚ØYž2ÛøÆ½zÎ2_l?¼Ê•ºBš8ãQ‰}y7i£y$
Jb­OFxæ¥‚_W¤“ìyòiöxEoð½fæ§D~àœ*‹ÌÙ.ÏvWØ!ôÍ«¡`³¼õ’§‡‘ˆåŠ‰Em0~ƒ¨¬X‚V.eGP>é¢ì
…'~ÌõQ«žù0èHðiX:Ï\Iàìì3€ák0|ÀÃsb»>Id@ÙmeÎ¥ãGg{Ô‰ÏÖÎ¼KÅ÷þ7g„ÿ×¾ÒêÒÛ~~’ÿ3•“±åïµ\ˆwVþ)Ì“œ#–ÅMm¢7& g*ºc:†#û§(—–ÝÅl² ó~"ÔÌ`\á_ø…ZD:EiFo7¨Qk¹8A¯ß‰ÍŒPGK%Þþv]8(Ë£›o­AâÄF•«–ðY'=ŽzñßKQ
ö(5$²Öª›—®lß4j—B ”fGo¦¾IÝÒ–ÇGK€Ê]KÖ&t
‡ÈAßd0	†;BsËú€b
ðŒÀÄL„è¢¬+ÌîQ… QkÝ5új­sÜ.ROÈ€45HËÈ;?"Ä‚L[Ù1sžÓó·~-”
æ¦ ÿM²ƒuÜ˜Ç®KÑ†)NÏ4µ
5e¡�šPu››´¼ûªññÇ?V#«	vT¿øcµKpuð±žÅÛr]À‹âiýÕ%1ç³G‘z£ŠÉ¢Âù™œ+T]¬ýºq0@(BíŸú2wçj£\ƒ¼e¾—Ó‚‚C­H1Øážù¬pRÀ²Ä8÷8ñ|à’wO4h}DQBèbI=æáöñ–pÜ3qÌ*;1ýÒ¾hô²"?“ðãGÏŸ*2\ñ›W©$['ªÕEš ðRSµÚX3§¾9ô§’æ¶ªJ§|s‚|hí2«LWå}:3
ckGµV’…’úwÜ%ù
9MƒY…09hÉŒŸgþ@+NŽ#‰mG’o¨6÷Û†Hl‹(Ô[%áéÙ¸ ¢ÐòçèSª’Kõ`éyaH·i?P)N–ŠÓLê€âå_G¥y„™—|B14·*[½Á±âpÜwÅ" ºÃaÝ×tl˜¬
ý§oN}%Ç "³¨Ú{¢:uî˜u¿¡4ºS*½ªòqú±¦RÆkÄ	ËZLÀÝÝ	ìtÄM=åõ 5¾Ážà0ªÅH'©´	ì_:È­ýEmxG®a¹É^ù8¼¯Ë‚±Qã©çÆÿµMnÖI„÷µ£~u“S7/£á-ÑDÙàøý–-
^«“ÛAA‰E²ñpgªê£…‚`àP‡2:ÅVsÊt¢ôEÉelv…ðÚÑ›df4Ä•nÊ0	‹ðÔ“—¦_[Œ½ ÔÄ/õ¸x¼ûÿ€TÆ}ÞWÐ¥ú‡I¿ª?«vS“,Q¯å¡ÿŒþ/ýåŒ¹&äu–Å_CÙ©—Â¬¬ñ¬áü}€éUJ‘¸\ð¨~tË÷ä sUÍ<»IãV.,Hòo��õ€`¶&¸e{X’S´5rÉd6Ù�*‚Å$•¹”®ÎÝ±Z–<]ñ«¬ý£S¹ÓÇ"Åž®H€7wë2@–î›ÎŸRÂü¿	)ë-ÞX<…¶íË²�ó¾´‰ÅÕj<<ŸuB½ÍÈ	©ª@�4Û'·K £xBççç.´wùI«ÁhÈ-‡•ŠW,96òJz%XuºR©Œ=‚6¹É`Þ‰Öo
báfÞ†™Ò8ÕMX6ùÔ#am(#$»FkãX°§„R<0Sæ±I¾-tÊ¤E””F|ótÆF("aAú2ê|uGåákÇyÃ•m›Æ™ÿ¬�Á=—®ó,fÄÁBÞÉm	ÓÓßŽcÕ˜ÈH÷,4í)'ÉHŠN[æXÌhJV­Y=Gq3V}Ý•±nUÅ_Ê’Ç_ÿÕ£*@‡|{’wLÌæ‹œ`Ï!v Ö¼"îŠ±¢@X²u®Œ£ TâíjÂÖ¢Õ×å@aEÚ«Y3¦°Ã)ùZ‚=ÈŠ´þ5÷îìbýlÛt1È©Ùp¥KßéîçK,}Ä‘@ÝáUs»=É®+øÿ…`$wMwðC–ÇK(ªÝí'žƒ[¹­ãfxVRÅþ‹ôVO…ƒ¢Y•[ý¨è™G7BƒÝÞ?)÷ézLŸw‡ÿ@ªïAcO»´ºÆÌšk³þ¶íB`cø:—Ýf!È­´õ
W:ï‹~ñÏû_Æ‚KÄ`wÇæ#ß¤IK@Š¤Ý‰aÌ†þÂäÇ²e!blôq(ðFÀ3Ijì Ò§$’MÍ¢Ý8¸=¦n‰ÙÞM—Nõ¶¾BƒzÇ–uÅÓÎMÒšÅ6èKUá÷@Q×YÙ�A
(²ÝpI	/Óã_ÜšbhòJÃÅ‚öÉ`´ö%Ÿãû”gßNí9çW„‰s3yÌf  í[­šPÑýªJáh¦u6Jð8eäEUŸ1­Ü‹ðý=ŽZÆª¢4€
OÞÊ™çünhçõ’o+‹%7Å·)ƒr™¶]¦6Ar=c¼Iüý—m¼¼àÑ‡¿ã€1<[D’(ë¤_€•5²ð÷âô)[$6$¶"±8E@¦§ÏtÏ™Ì‚ø¦ådžÊóJréàCŒ?ó™ãR˜ò¢ªmÕùg¶1v¬¨ÕhókGÏ8r—ŸNYàãä,/I8Ì¢‡²ÞE>Î5…–r‡ñ»Ùžá‚þÉ?Tª|ë58Úé=j…|s®ò ;ŸkV±@U21¯™+÷Ï¿HPÓ�µ….âbU˜�«yc¥ÿöWuçÄP–Ž‹Ž}H9¶Å¿7<¦žÇ‘][šeS~¿VHêË˜ÚUÄìêEþfîçú„•Û®—]Œ'ð<XÍh½*ªA­]=Q•:lâVDYá»ëªûyh=
ÉIpŸâþïúÇ´#t šZXrNsn{mÄ¨÷ŒÂ
¸J5ijÿMO¯q×Z(=Gµ°Ùç™Äà™†Sâ®ÝqcuM‡TjUòkÝ¿2q";²–RÑ%¸ù©§GD{T—$}%˜W
½µPJû(·fCWý*RaJ:}…cH«œôÈ>=ªÎªháàÑ;¨ª#ÊÄïí¾ü•«ÏT¦P�ÿyÿù:Š·újZ«I§YSr4pûÕ¡*Gº*‹)(Á(
–…ÚÃºe­f‰Œ:z½ö9
Íè~}äí»wŒã3¡¤ÌYXÐIÆ
ðÓ[1Õ4øÓàè_k7òÉÃ>ß8T\š2Ëv®:ÏêN_ÞŠ_½pU‚$¶Z+¬2Q±‡l°~‘óhÖp‚ÌWÈlrnAòë¨ii£/aæöCô,JIh„;Ô©óc!ØHóÈ`²™Œ4qÙ³‰•-)¦5Ó>â‘ËÆ4@ëÛìã-79-Ë;ÉlA.o¥@ëåÍcÕÈYj5€Ó/k.Ä`·¢¥	˜&v%ÖeëÅ‘\ÔHßŽÚÈDdªN©•D>M\øtHýþy•"ä;w¿ÆÒóhNB^!¦	å|*®“e½Ž hÉÔãä„G¶?>üµÌ%ëN‹>¾¼4jcæoÍ6+c„à $qbÕÐc",˜ìÖa–Ñ7™{(çŸ�ñk%Þu>ò¾Ã–ç´Ý·‚Qâ.EY7ƒ<d%ºI§äün‰´VK®ç{UD¥¥¬Ï­­|Ú˜›â!&Ãßí2FgûôÏ9p#Œ$[‰åš|˜·D•”)Ôì8ôƒüré´%èz"Æ£Û<]Cƒ6qÞ€žÙèÊ¹ßQê©üÂÌ.!¿o¡mº­˜—€67+]z‰eYÔÃw®	”×ºùQ ¦uÔà/ÿñ,ÿ\U\7›¸éÁ™]_¡;'³ãÜIvàNBßl³A½F6ªÇß{©iÄ 0ª&”„$)X€\.¨e+arè\°¯ø›åj�P�ö~£ÓKýq!–`ž<mÆwÃn„Ïµ&À s&$à;`1GÕŽÂ˜¯£G—-^æ5H+X¢³þYø%!¡Àdýÿàe²õ ÖÇ*%¥!}Û’™AÎÚ•ûzŽð¸(F‡ÿ"_JŽŒØÿ>çu:Úh<²›ZcQp¸ŠüT¬&D&�|¦T»‘Wƒä>þÚ¨Ã±zšˆî6/31Ïí›CËi^£¬ÞKðÈi‡8–LôXØ^èOÛÆA€ÆÐ€	poOõ
UþÜVâ¿JD’›IÇ:=6žš
ØËÜ‚8 ž²´]†¸/ó§'÷ÞRÔ²&èÎ‰$¨l/) T¼]¤ÌÛð&¡©d«»›p�p%#ágñ×l4ÿgm€gE¹Ë·Ÿ*{¿Á?eiÞª‘42EˆjÝxr8l¶é´Ú‹=–ß!

-š3»4‚Ò	]s?1%»�#À½‡TÕÂ!o(Á¥n„x;Îã¡Nz°Ôã7‚-5&Á8Æ/cÐSc"È!ÍûR¬D~pÖÀÖ´†l6êÞ÷[ïÀ
p\„ß³RUbÝP¥•·3mò™Ž4ìÿŽµÞÞŒÓŒz—u0ð‚�–D—”éCÒ&’ñýŽ|×Sž6¤È=zvT¦-o8’xc’¹¢ž<“"R'ÊåÎ1†(akŸC'èÍÛåoÉ•ŽEÕ¿h2îùò„dáP¶GÉÀ&ƒ<¤C•Ï\äe½×Uæã�›’÷|2vpãIâ
Äk|YBA÷Š÷N5DÙ•[_Cã}$!†c2
ŒUU6ÊïƒèpöR†ÓGSìa ›qSò~jÝ!øUDÀ±èä«žŒ÷ Yÿä™™•›Æ"”å1ÑÎ')0§Í0L#~³¹;O™Äa`l$NJú³þõ”¢–ÿSÂÅ©WcëxN)UY1©Tèn„FR¶ÑtvÕ¯ú<\(”™‚Tâ5&Ú2îYõg±–æ¡×P»ÂÞiŠÑÔuà87Î`[™O‡($éGN‡&ÀMÜû€õa€ £ÿØ‰9V~y¿×©3vïºû¿ôÖœ ú&P¹HyªUOÿæ,g\Æ~½›I¦ÎivÓñXÉY\ma(YZ†’´épgbŠ?{2°Pêÿ¾iF°cZ2û3Ìõ²dkÆöù¡TÀ\.Æ'Ñ¡]Â1ÿƒ<¼a¾B|§õZEëùb’pYg³ô¯‰Ð\éÕÏ@Æ§ÍhM@…ÆÍ%Á”=%1ýœŠÄ{Èx›Ø~<
©HxRåëÁ+`Pð_“tBVy©¥ÁT¢Ô:˜ëÂ{å˜2Ú¼¥bI£œ	—£O°«[ì8Z3Á,ûâ Oí•åç°Yx)<…Uˆ|ÄÌÓyD˜¬‡DP ßÊíAþÆ3X¯è=ö@Dƒ·I_¸IÁ0û)
÷n`ìÙ×nëäúã�ýTá¬»5¼]ÕoÑÊºoÕÎ�$þ°î™1.\Oq¥áu¶92q¯épK
€ ¨xL–­žî{_ž¬Ìƒ9M\õÁf¦’~oîW-½K8×YjÝµTÏ¦X6Jœš‰g0\b
Îxi^3q
üŠ±,¾ÛÚ5gôÉ§6èüïö‡bIÍ<Ø£K ©öz¾æ¿Y*v˜ÛT+6@ÝýÏ©iÕk=Ñ`#P”é…°‹ü‰uòÊTrlœL\GÔ#ªx•¶Ò_Ò	,“Å·¾w€ŒËæ[þ�aáFOðäŸÌ=À®ë”$”$OS	G®»g˜ÅïcŽŽ*×` $MwœûØ}ú¤¿Š:»ªuäé|¼ÆÎ‘+i5ùŠŽ+…U†¹*:Ûˆý”ä”˜Wø8­i¨H’lºpâ@–^à9Sl±ªì•ð{^Á3Þî.røY+{ñ²wÛÊš™*—)†Jûzƒw9ü+âEÎsÊJí”1¨±]ö@Ù}ñß¾ÎÙe+åhzfùwð'w<*b)c®óYbplo†Ö{-Ã–wÕ
) \u¯—ïIñ3c„Ú Œ‹¡çA.röšõikŒ‡²õõ¸mìµïgÇê,)TPÉÝ!éCrq[†(…§’Z¸å@ÈNm¨†ëº“ú|»çUghÛpˆ/^OœJ5^–ó€ÜBž˜Þò„ñE´?¦b”WE-¬ÚÆßÅ²µ­¤ôC¡á”ˆà{Çlæ9ÙÕþ÷ùóëúqÊÖGªxDþýÆÛ³Á�8jaEÑªÒÕý#Ï²x¶OZqyÀŸ¶„ý´8°Ö’f_"¼¶a­NpôŽ>É`åÉÒ
ïŽ44ïßõ_bì¥Ím9;ïÁ†´È]c~º�­ù›iš ÒV“iÀ¢‚µ]Y�AŒ-4è2LÕEvcòŽîºÑ£ßf8ßI±Ã{1žcÉMDP8Ïm³‹I¿œò˜¢zÝÒå”Ê0Iwž¿é&qLÀªˆ­;ŽEÚg*d:5²Ú	ÃÂÌíŸY*!á˜yœ�€öïÁ—_÷\À,l$[·£…‘Ë4ù­¾ÕL«7“ª%T§g
€,:Ù˜«RLÞbØœ•î\1ªyëF§ŒT«rÉm9úRÆõ”æ‡£À¥ ‚Û{ÊûË[áFÙ=“Ó½­¤ù’¶…Kx{‘uý@
íx6%mc®\ëwcæ1‹å´ÂºÇ‡§2+z
ªeG5p­.p¡Ñ2ùSí’=ctU`¨²Å¨9òÒÍ?»Å¹¼+¥ xÇ4×”@¼ÉOP»¦xQ×Ro$‘o#W]‚vuÞ€~e#¨hžD¬•+QG¤Ç8WÇYFh^.)e$^–.eXA”¯¦8tÃ`v¨§5Ä†Û¹áíTFo£¯Ì’pÝ~*q2a›ir`/†Äb"¡KáqXÉgM_×ˆ2HM½«_=f}WvqŒšª@m,#8§Ïï"Éî<}Y‹”ïÞÂ;Ð¿h…¿¨J™6«	2QZ&Îå¹øäÅ©Bl@ì·¸BÿrÖ*¦Ä˜îx,3Zþ¢àÂ™À,Ç85Êo?q]\Sïö`¦H­àÇùIua*ÙÀðxp´aEÁÞªw"Ý”¿([Áá¬èw4%p2¾giIy÷¢H‰§<¾Üø2„$)‡?ÓâMv_íD?õ<€Ü�\©H‚4j‘#LÕ]œ×oæ(ªÑÐ{?^Òž>-4ykºÌú©f{úÁYÄà¨|#Ãa4ÂNl$©ü>¸ºì8(êº<hqÆÂ¬0)¼xæ›{Ê°è­ŸÊ¿ëÙ„[^*»‰Œîx¯oÀ›•“²ÓÛ~Š‘7êlÎÏôaÞÒ"¥Ò KörkÐî›Àà„\b?>æ‰roÉÊZyJÝ§3Aâ…óº«=ƒêã“ÂpYùVêÊÉŽ~aiÙÅ?Ô£yÍÆÃf›_	ã@\Ê6dûXJÄAÃ¶þ¹a˜™cÇ<3þé¦Î^Ð==h«±Üœ®‚’né–dÌá±-oFøZgk'K,‘µ×&\Œç,à©l¼u—
5¨ÙŠQ8Ý)[t‹÷‡ûNëî}—å³
bä#CzÍr¥7XLTåkÆâ ÓI‡¢b¤+ø3Š¥Bo‡:ÐM`A†2Š3Š­žÀ§zÃ;LXÿ–H_ò)5ÀS£-4*IJ£|÷ŒFzA¯yDÿíØ#e…X„!·…�'qŠºGbÃÚIô¥ü×iù–íÜ²÷_òGûYU O¯JsîóNUýð®%u7Í~ÅC*ä²Ð–íf<(Œ¨	–¬K}[Žo£‰Õî`0Dâ
ïexÄ4¶êà³6ç{Ë›ˆÄCÉ†¤;–"j¬‚ºåÝ>Oãx¢xJ˜Z¸	,Ùbà,â†Xžù]²z…œŸ{Œw"E¬¬U6	ö[ÄÕÁâ¥»à¬XûÎyJGôÇ2ý÷º™•}ú¦	¿Jê=+Ãkßö|,P êY·«eÉ^®3
ÌÌ©¤øâ'
¨¯ÛXOäõr_íZk;®¢×÷©
C¿BBÌ&j�jŠ^±ªz”ïÜ–-­r1­9ƒ1‡Èu=ÞN•«MÈ}a¤B=ùYŠq>+ýã™ôý=?fù¶;ôjžTewôšÓ@,$“h“7º:åSõAóÿ–”Ú°Š'Í(WKºoe¿'¨ºQ*‘ŠðR§YÔÊT©Mšöò•È£µ–î<¢ÝTí×p»ùYJ[JV½n$lÂý‚Ø®«m˜Èº¨‰·v•¶EQ@Ç´À+	}ÞàCva—¶èCcNH¤£Õ÷{aI÷«
s~¾%Ð–ü½9bžÕ»¾¬Îi2šŸ´q·WÃð‘!Kþ§ü
¨a¨d\s¨ %"¹î¨•µ>W1RHŽVöMò´„	
®¬ãÔÇrRž;±õ˜ T¸ùŒN<Ì‡›ZqŒÓ€4±C4‰š–Íì—àìÄš¿åš\5RÀ#¿°¿¥I=üÉóžìMÖÀÚš]4aa8A‚ð$÷•šZS’F=/CÙÒ3¶2äo´Ì…Í‰äzvçU„è)W‹‚XøFÅù3‚¾¢mAéð#×°<®¸@ÕZ»l>†°¡VëÂI
")@‰éEò	lÛÐJr;ÚG¼/œlÿ½@÷»$jÊ§ß8«ø¿õüHZyyœ“X¦ ›ev>{¼œñ9	 Q„2cÛßšE«*}ZôÚËPù\¿öø‰uäQy˜ãcÊBD­ka Çí¯EVÆ ´óz1ÂØKi‚íHaJñ8¡Eùb0|Û 0HÃ¦oýQù„‚‚¦€W3äÑV «¸ü^ßÛœ)ÕN´¯HÛ²0¡›HY1±²Ÿ^Lzý¶à1â®'æÄ¦øfx¸(#s`²Ïo¬T»öp JK«ZÿEsŽË>“M–Ï^¥¦=à]#jƒÕ¢¶ÿ+
òø¤ï°yðiœ™íq	Œ~ßXÑm7Ø¨h‹a®{|
mcV_s·œØøÃŒüŒçÎÔ!ðç}×Kf®O¹—>J4ƒuòÉÌ6L &¸
Ñ"ªÚ3T�[>Ü_%Î•À–X;2#’Ö6†2Hj«çGÖ»è³[#¾\ñõ›_6Òþ¾<Íã1lk™v×IB¼^`NMRQ¾CWÍ°R4Ï¿*63û´€Þ¸1žl…¹Q³Ö0³\j!ì­6fx
ùsjœ-£!#pzÀ>é'øžžü¬ZâñÒ‚›9�Ídì»íG”Jôb´±âSÈ+`}äZ¾Ôœa›H9ã•+¯À«ª’m†–ÎÈìk}wú¶wÓ"á•Ëån<ø›¬È»EÒÅ$xÊ†±Œ¾^`sòù¾@x˜ËK“˜‹™9h��Aß7íÐyÓL=–â|4“„‚ëƒÙsŽ¼Ãñ¡gf‡N¢¯.ŸÑ%
Üºši®Tául=Z©®¢¯wš
‹Ú^^ÜÍÚ>>ŽW'Ú8Ñ½nCätd–ú(ŠƒÄ9c“ÅŽ$´â“ç~qÀ(ˆ	ˆžó)b$'ÆGC·„%@ÂÀ¦óÌù›4øÝ-`s·Ð¤UX¹eEÖÉªtº“‘ó'*,2ã=Z4ƒÓ×WC¤ñ9ÌƒkLt±XÓ:~‚–¦œ4–J7úû”®sA®Iš)Ò’âüy/L”6WéŠs­ÑÊí%,8Jý-üG]A×B@5ÊÝÌ…8šÏb‡„Ó•œí¨éÝ~61LŸ?ÿ7SfT'Ov²üX±ðkû€€žÉÂ"5Û¸øpO«äÓT¿èñQ{¢Ë’©3¤•ŠÙ]þ(]¼RS ß¡ÕÞÇ3'©–%ÝÞv\�K/UšÌ Ÿ“¶z:—CÆï?�1ù7Å‹•öy3ž²M’Úô$>ŸY!+uÓË-irØ{·{«ÀÔõ¨íìûõüéâlë3ãÕÏJ•^ÑŒÁÅÖÚTUFG­ÒŽñ†½D;ÛÁe“E=®Wð1vzš[F>£Âçå¡–	l—ŠÇ Ò2nà*RW’ðá‹Òr0SÛmy@òëY“XìW-•J;<³úÐë�‰âŸ½lŒÿÇUÛúöÝS$¹…Ø	‡7x(àÝh2ž%³àÄ?Œn=­¥Â”¬·ßFíÎÆð°ÿû°¶	æÜ6É«›hK#R‹¡öõÆBÞÓ+ïÕ6aKø+?÷Ë®T(C´a–ÿ6h@JI\µ§˜Æ'‹æ{¨q*ÄehéEÃÜhA§§RÆÑÜÊ±¹!Ê"‚Ã³50«„Yþ$7$´±h°úGÄ¡uòØÅòýZý—²ê[Ñ¦œVrñ><†v^ªi˜<ÛÈF3Î÷oöeavþtzÊ®:U~6øJŽ?·ÉôR§wyà…¦í-Çt\€ïhg,sÎÎ
å‘žéTËÛhnt5¯ÜÒ59ÖŽž¹ô¹ÚÎá©½Ê¯Áäœãb	ÄÏû¾|-Ù”ÐÒ`%×)¡1¶j{cÆµÜâBBTK_ÛÌÅ«‚äûiæ5¨‘j+§·)ó~åhÚ\6€"Å§å1,MÜž´ˆÍëØºi¶ßð8c\öS‹VÃ%qÅÝ+¶ÍÇ±ý¾_Æ´¼hÚ“-ƒü‡¿D­48à<†OTPÞI}-5¡´©K{´LZÐ÷ef¸[Ug�Þ&ü€ê‡£5´‘©-;”Ršn›
IÀž6ˆëÌåNÔ,MzG	Öì·„•‘nüA)dav!Ã§´~@T|V)èNîbbM|\K¶ï…Æ<l¶ª:†Ûjý;}•\¦pÁÊ²æÑúc?â„ÃwZ:+Ù¹ ¼?9kí�€‰šNPùî¢95RîoZÖ˜J­÷N´Ï2lì¥hÏÆø`"’OÒoîâZk™K±›†_ÆízÇóŸu(b¥Ä¥½ýŽÒÖZ1“3Så-%	c{Þ…*Í‘t Åe†@­Æ9}eÝO]u¨[n~q?Í%K{Þou'~kÒªâ»[g-g]¤W _¤vzK‰È®ÛcîRZÖêï¼9O}Hn'sÂ‹§éy><ˆ#Hòšr[¾{‡‡x›!ö4ìëù.ó5ñÖ	2ªT[3)x­Y\€V‘k·þmzù8ÍPÄtÒqŒÏ]Ð4½+@Åâ§¦ÒûR†_Á‚·øL´ót©þ_Yå6¸š«»8†áqØ³‘f$TÆW~‡±ÜFŒÈR‚¬Zâ3—äÍ™¢qGßˆÛmsçÊ"¢ßo´I¨0
öÂ¦¾j~Ïk×2L+y7{?67uÈd@÷òý×<ƒ 
»`ÝÙ$…3­_äW5ÞâÙ¿ß$fk÷9‹$a"q§Åš |)gý‚Ðòr€´ÉuÅ?ÄZîYÚÜ×-	
É;ºîªc¯5wð»8JŽha­4[ÿ§Ó"
^°a"Mº4Ðì$ðõ>`Qøþ×Z7x‡©Ôk8µK6‰dgƒ	Z£'­Q’h Ö—�$ÒR‡¿$ÉŠM423h×àÕ(òþ›ã9* 0->¿zL°7ŒžêÐú’Z©3£Bÿ„ª®´øoA‡à^Ðÿ¸iú´‘ÅøMÚ‹KUÇ¿üÚ+
$§Š~</ö¢ùÆìà‚ÌUìNúCíTð‰Jb’½R€V^%œŽ˜£tŸ·:Êî~Çk9÷pWì.¥2Ž‡Ga†kË¡/L’ÊØ:èþìTYXÑVkjbküŽxiŠ+ÛÝR¦(8éÝ‰IÇÝ£ú3/³ÓÂÜ[†E
ÝKWº&LL.T4Øö0g^Ät9e`‡ãp¶y,ý!óü¾ÔÅb²Ä´R˜w*Q ÛÝz·‡+à)w<nØ*Ë[Dï°SžÍPH¡ô‘âFâ,“n²ôõòìƒ¶pbOÊê¡™ï<|CÇ­ó¸úþBªwLý=ûT\T“ÿî3i¯—zUhl'²ºpëÃÝ£5×‰ÆR”l/ü—ÁèV¾¬)C·_š]PÓ1z±~ŽEg;å&:ÅÙ0iôSéÿÕ’<œ Óö<?XéÉð±ÃY:ð¨×¢.+Æyÿ!Ýë¦x¾4äåéßahfXgepJMF¤ŸP�Œ ZµL+nÅ]ý|Š9,IN_·OvŒÇ‚ÊÆwË¾6±©dò‘+ogá2Â
ûGˆ5Ó‹úóˆ7˜—äR¨‚´×hB©:ß	¢³­ku"Í
¸Wx•+Zó_Y`Ùáþ­F)Ÿ!r(P�#úmæ=uÛ‰t-ÔêÐïQ}ÒŸ´ŠƒtÂ îWä;Î„ø:É\õ'‰‡%¦×<µUKÑòg¯fÖ±ð9‚¶´XÄ>,N(fgñ_ÄjyN+î+‹õËSF>;CýWÙÉÿBZÆB¥Úp@¥Ûã,nÞ‹0Rfjdä ¨»N—DÝÒýfÁJ¶çÙÞõ¦»t¼G½·&}?z™áú”ŸSÂµ€×_D.ÓŠÑ‡F{ÑÀJê²i»Ößú ]ÊýŒù»7€Þïb!—W‘ú¬ºvAMk§Vâ\S«ˆöÓK‚Öýµ|dé+LÇãüL‘¢A¾°‘Ìÿ¦á…]Ìxúo«`R—…Íç	ÚÅÛ†i‰äïßDÙ‚VP‰±%1:ZuiaÛÜõkêïV	ÃVõ4M6Ê¼Õ6o£ˆ˜tžø\*ˆK£;`7Ui¨êï�Q§%Ê†"9=Æ‹O'ÔŸE4"ÞFkÄ/“4;Ê›ƒ„‡‘hz×È™.9X-9
v<ž7¡»f@¸°Ì_ÃqÍ­
Pà$ôgÿéÇ;{ŽØtñL÷ø„ž‘—æÀÕý~Bît¿¸§‹›•2<fÙqŽ	Aü˜ëúF¹Q A½ûÿ`YæÄû¥Ö(Žè£>UÀ5ˆ9 	øgÒX£Ô®0®%†/#õÕC¤îJÂP>@*8¿d¸Ú�Ý­]êªïØ\§o•Ò
ö8T3Ä Òén…R¸³m;zÒŒJbÆý—ä4“<À{¤'È3AšãGôFˆÀ}®qMƒ7&—nìPâÚÝ'éÜëG}zø¬âZÕp­qCäód
AØ›@Ãüf¨îøÖä ºøÏÃ%ÔÓ¥¼]ê,ðlz2ñãJ¢Qé:õYý¥nÀðÐ÷[
¤¤õóX\XÃ±8©#YôÝÈô•hÚÞýMÎš)lÈß8Êb(Ga•ß©¼©V¿ÓR÷Q‡ãc/óÝ÷¾
–.ZÖOJ°CÒ2qá‰B°*c…O?iËrÞc¼.b’K_‰Ü©0·³y{í˜rfOPò§?“}ƒA¼à°_€Ë	CíÑ¥_£j4¡@¤}5Púp)Ý}³à÷Ë´1 Dv[áõ²qh»Ü–	ËÆïÄ×˜ÉúÜ·Où²n�@ÄäÏr„<œ5áäÛ»æ5KÚ4Â*l£â˜·sÉý
Ú3¹9Ž÷ïó	Àø†M‡Ž5Z Áõ½‚ÐA3·è–ÂbÙÑsYå6ŽB‘ÉŸZ
(7U!oü¥ºèJüõNí *B.vTeá¬û©Mãf­ÔoãGámÚx
endstream
endobj
41 0 obj
<</Filter/FlateDecode/Length 48>>stream
oô…Æ®Æ8éñáV@;õ˜¿´¬j‹lD¤e”žéÚx NÓr™g.ç<÷g–,ï­_
endstream
endobj
44 0 obj
<</Ascent 750/CIDSet 46 0 R/CapHeight 631/Descent -250/Flags 32/FontBBox[-502 -312 1240 1026]/FontFile2 45 0 R/FontName/HQXYDH+Calibri/ItalicAngle 0/StemV 80/Style<</Panose<19f80ac34176774b8f2534a918a748bd0598783fdcd03faf4ba42a12207ece95>>>/Type/FontDescriptor>>
endobj
45 0 obj
<</Filter/FlateDecode/Length 37104/Length1 104008>>stream
ÎÙ˜BE›ðf§3èeÙ^RÔ„J}ðµ(-DÌ|”Ls 	Úé)
|¯*R
´ß`:™g“MÞšFdØÕøü%p¬^Û&™Ae÷;æo¹ûâCAoºÛèýÉžÁG‡D=]ƒ¥wmh,µÓÈý|6yTedimFzÕú!ƒGÉc÷€G±*oE+ˆõ‹
$‹éü:Yû#¢‰y„èKƒ°–úz(¦•–ˆ¥ÇŽ±S#B²ˆuƒöãÖÐî~=¶¶÷°VI9Ð„·!ðwbÛž¹‡¬¬_ø*ºúw°#ÎÇ9ÚPªØ ëlÉ
ŠJÑ-žý¦#ôy¥ù	üvûœdw€1ŠkY´ÿ»MŠ—~ÒßT‘Ã¡(Q®ñÒø"ïóµ½B�l¢„§¨p&\Í¡³ýTWÚJD%	°•‘‚Ývù?<èÍ£`¢üX[Cvö¯mœ×[©ZÖŽIâ%C!²ø^ÿMÖ™îŽf‰�bñŠ¹dO�_Ë¹kÜ)qB$å´ªˆbê¸V£ Ï‡Vø9T‚½†©¸˜!ÀOn:uòsàMÔ|á¼Iü*iVŒF¥äðwØodSn]c¼ºœF›(§;’ÊP¹ôÀ`i³,d[N7œ¥«K_šå§ï¸B5ey”TïÞ��!=Š‹&ÏqäV�ÂÏò`[b§{òŽTóÑþ±áÜ5sdÉž,:Î®¼;Ä�™Þº§»^pM£,'§L;®º…ÒÎõ9šYŸ„y¥a+ì"½“üJ¤&”'ÿƒ~ñAK 2´ÆüBÈè³*ßçaÏòf¥¶ù¼¼aBd 'ùôŒÕ›ËZžxC.J,W•~®Æ•ûð`ª1k•î±ç´xX5F"
÷,øëŽ¦ƒôÛ”¯]³n‹ö¨Î¡æÍªÊþêa
[œzRO„A¬§®<’«?!‘Ñ,y1t!*ˆ‘.Œ…L]ˆ÷§ZÙ-jïîzŒóÒ­y¡VskuªYp;„eÎ*éé¸ã#âÂÆxñ9>$žl\B¶éÕ«Mïq{èæ¾;²’Î¹©¼C*›DêªÒ~â
VÜ–?¯ÇÃµTªqE‘l
.
ûÿN"[¥ˆaòeÓt¸åª`EåOâGWyÇûâ_£¤“ÆÂ"‚Ë§Ÿ|»Ã}¥Bg.æfÉ¡Hå¹n%‚³™auÿuáÒgê<(€eßŠÌ—´ÊŽIk’=š’_Ïi{éÆ§f±™ç8sGï)ŠÑüNŒ×á˜>}õnê<”‹D?>R„oùüî¯,ž£7”G×$ËMÁ/¹Û1šÌçºuªBÑÍ+znjî	lƒàÚ«xÝBc\Þè|ÂsÈÀœxQáÛ|˜êÁºH­÷Ÿæüp… tEë‡HÃ'ä¡èùÑ¶&„*A$œªÂ®±m²v.Ò¸XŠ7IW…¯¹aIb›Œ•…m¾˜êÝð6óGwêçˆ+§Úûr®cÁ¶ŒzÎY„¿f‘#0„T‹\vãgDE}RH±Á”Þ–­f±ë’¹>�¯—Ì)>£+Ä÷Ó,ßtÕf{O‹É¼à¶z%Câª›…ºGÃ™Ñ
ý&ƒë¨!.&™¶U¤F+@R¹¹ØuÊë©™7Ÿ}@IÉ¾Š×¯Åi‘JÆiƒæTÄ¾­ÈœÒpn´sÁ	ÕAêqvÜÜ LFyz
lÖ‡W0“:¶ß­­u/ç:…îÌ\~ö8æ)Aþ,³{&š¾}e2ŽÑ-6Ù¶ì; z°Î‘fXCÿŸ…0qx#É…<ãnu`u[§Ü·ï©û¹œß³^þó
øõL—F8é×Hí£V,c°XzÞ±ËÌ¦*Õò¿½3È¥W(‰¹
¶”“íê§aBv*Íþl»öx]ïU,Ðð=½“Ÿ ©ÀEý)ÒlÉêx’%Œ5úÿoç=>žnZË¢ˆ9ž„Â2í„üÑCEÛ«@}�øzàv;1ùôòZÔTŽûêO¬24: Ä w
Cdâ2Âz£¸ÚÓf:(#dÃÜ‘q>øÑó+êæT±>NPK“±w
¿Ò£ÅJÊc;`É5åAëë­‚6ù_°�?¬¿ˆš¦ˆØ…óßvŠäwL<cpZð"CáãæŒÉ¤ºü3ÓM$Ï`òÄÙ¨'açêFÐÂ¯í.	¨-‘\l^i—¶˜5ß{T©ZÐÔ°J¶³ð+&_–.&0^dùs^GÝ•òå
°ä.×iÉ±¬»Ü­ ÌÍñ< ÝÛ¯]c•î¨•gwhmÉ"yŸ×4RNµì·æê\¹çƒ0:^HUyÿ�rn—s\@ªÿu›éæv1ö™2›øéÜ£ÅH’¯‡û+™¦|â|åu×Ž&†*%?
"‹³ËoàÛ~ìæD¥‰„,,<ÙÆˆ”ª
ÖD³ÕsqkÕûƒù¡2~¬O´/”¹ÑA¢Ø/Ø¬T½Dlj¶ˆi½’£5"äáˆøBšªì},‚8€Y¾¨:jEŒèºà¦Aey3 ™ÜºÇzMÂ{›­–Â·™à$¤›
ø%}­Ëe¹Æ`8SòG’,ÒÔ¨ êÙây=º*gÇÞnúÃ;ˆòýÅÒ¥BšÖJy8ŸÒyRZörö½nÂOä*ç8¯®RŽEC\ÉY²h)¨k°‚8¯‘H„ þ Ë`Õø5C(vè£†¶lÝÿ—²Ö{úz¾qEy<`vL3"7)ÆÃ]rÁÕh½³^n%TU=^û	Y|h²¦Dðýr‘=k§øK¯çË^‡zÜmc…!5b•fÂ#'>Í`_)Œ²œ=áýØý¯Ÿ\Ý~|ËFº°‹·uÇ€	þ›»fàÔ-Ã#¾ÛŽÀLrŒÌ´	C¬{ìs¶¾ì”›ò¿à ‘§@yµÎÆ@ÏkÉ¼\OS•×îWªd;*¶Û*÷éŠ"ÁRð7iÍ	‡¯9¦&lˆüµK–F¦}ÏxùØ"¥�ä×Ì4p‘7¼cYvÝÑ4Ÿòlçƒ%¿ñn–m¯Ðö¨ïxmj›g¸EbÐ£ï$6CAŠ‰° nøÂRÐ‚ÀŒí´þGÖªUvˆ‘¢~è8EÁ,VmÇ;Õ£‚o>}þÜ±Pg×…Í‚ì6mòI3Î­¶c8C¸çÅ5gL”à‚cÈàe(ã‡óGGÏ“gJ®6Â}|£SçS‡Óv10lðÞJ#ßàÝ×û…b‘Š«Q*Ìsì=èPÆtóu
	,1U…Ô¿0KžÍÉ¼>š°œ†É/ÙA4Œó†ìKiðÜy¨A9VŸaž—fœ²Óè¯xû³Eý(Ë.±Áÿe÷¦";ùq93Oó¨ÌN¬3¹á‡¸Ügøì¢9bÅ­'þ5zÄÇå/Vg¥ŠgHÆÝ|­õìkt¤wæSNä?åb‰8¾Ð˜Ô
`²{/jþ°cSæËLÁœ\ZöbúVÇV]9Éð}ë¼û£½Ž%Ù‰u±WlN‘¶3+~¸®ù4BÑã*ðÔJx4ú$ºÿ4ç¸æ&§kuw!ñ~9C÷õ›CäñØB£¡¼Omœù¾ºOÌ¶Ñ]\³çU®E‰"”,_·€éõŸ×áµ#è$‹O©¢?&œ—”—Äž‘Jwád}áƒu;Ñ‡˜X°�‘ç©O$íã¨H¹Ø1dEVÇÆóÇ".)î”æ¤çŽ‹|n8/€Ö ýTç]¥>l¾|R5t755Ð¼ýç’’£9üUròJõ™™À “,÷¬¢…€éÓ5\êÀ&±œµÖžÙîÛE³½gÑ&I¤•QÕ+‡ ÔÝ´âÉK”W#jø«ˆÜÃœŽÀ^•cÔ%è…W²H»NõØ	½Õúça Ûvž¬†N+¡5ñïŒÐ‚r#óá´‡I¿ûÅ8š—ù¶)ãE"©.ò3u§¢L™MÞ|·µzžSe®)_è¯¶•tê¸�$)DÄ÷Œî­>PÉhÃM½­[Ò-	¾ºþš™²Hœ`ï‡Ñ˜¹>ç-Á‹³°N± ØqlU[¼B‰†À¡íÌ¢£S­N£{u?§{³,(z÷‘šl/º|ð”#X^	ZhÉf/çŸŸÔpø”\¢l“žßãÂ2,x±5MÓïŒÉÄW‡DBw]EÎÿ&y6=bPª›áóËaäUÍê@¤u‡ï‡¨þ±Êù#¦DÑF#±ì4áö½4¶W²éÎŽ-ãD„¥S'Á½[%AßH“GRp2-´óhD`Îœg¨Ü½‹¥\Ù[ñ6œ
µ²K3ä®ú¡”8Y=lk_ÒßóœƒÓ~ÛµPÃ'ÝÛã@påÞ(¥C©Þ´Ùx„Ø…Džõ—	î\8£½uw•.EùK×Ië×‘ãß£"3<*f¦î¡c„XÚ®ˆ¿)Ñc9¿½LÇ\VÒ¬1/“ŒÝo–ñíÆè…(å½�Pxñ”ä™CúvÜdw¹1ŒøˆdÙ‹wŒä6ÒÁ•µóP,ÚÙ·[´4ÌPÛŒ Óöf;´!Ùœz4e»Ed3ëøœò&¼k¤Y³
™8‡œ(J··šµñÀÜß-—™Lxê§¼MôT]æ3ÉÅk\¸gm10çƒc€{©f›R[áŽTÝÌð”]nø!\/DïÂB?„0Å5ÌY-„ìî¬Ñf}üh·»–Oîô£¡ùŽ‘yGóý	\¥Í®ñ^A�g1_„Œ±®)0#ÉAÚ‰G	Ý~Œ’ï…ëwûÉÕ¤µh”EÅTýa¬}=_o,6)³€CdÒæI²çdz«¾¬ñí}Âî—P©µ¢s”äg<z™uÃF~°Î×l× FüxòVÊš²]0Øª>“g¶üÊ‰ÉÉØFÌ1ÀE“Wt_Ú¥AË|À‰ö
Ô^6)xº¤Šû¯æ)y"â€Ðëk™gÅ›�oo	I[t°âëúÂÚÏBÜ¶èîXq¨ßM%C~é@	uÝÄv[­á]`¬DìÚó=—U+@g¹»þÜÚ5€#°ìˆÿx(ÅÔ´‘¹cÀÊ^¸ç˜¶îõUìo'Ø.pAÐ2ƒŠÒ“Çz`’8ønóæiTI‘¼2	.]à‰ÝEp“ak2ß¹æ¥n „	”*Û
Ð”ùáYäû.UÜ7“æœ¿Æ�}÷àl•õæ.ôåüeÕ‚Ù+¹”tÿ3§ÇðdS´·-kyd;XTi¯eòÖ› Þ/ ž±È·BàDSmñ.=HíÜ>Õ¸Sð¯èåô»Î¶k…S…î1>r§ºØäÍ´­¸P7–Ð‚¸f'5Ì–„UK‰¹F,sÁÛ³Ñøµä	\a	Ñ¡.†äÙfz¤åÕÁÛ½ˆ˜÷ýÕÎþ°èµp¨~õÅ­ÇWZ2Ã‹ƒJÕÔ}ŸEÁ[~vS¶=ÙûªêÍ&B¡
ÈÁ‘.<P€ë€TéDäl„ÜwÈÍØÜÈSXÖ–"üZÕ­¼.[ç¥ObXŠ3?÷v$†Ût¤ôV>%Ýxß’mªyEÛ¥› ÙÙÃ¨Y%Ž›3ËÝz³¤T†	ã
 Ó{>pïoðÖ~o7c½Ž‚
Ø%·m|“¡ÿõàUXsb_V!ïÈ"–ÿMW»Ïê
i“ÇêZ·Ädt(uïw†HeÄÉëÁCÄoE2s|ƒÍ¤êT6³Ç¥xYóæE{ÇýœN‹-“wÓßC_±4#ÇÈM¹¡Úbeñ¬ÍZ’7\—Oô\ Í/×õ¸XÚ³ ÃJûòƒòz59ÿ²Û”0þ-…‰2%vVjµý›«¾`tþ³ôgã
HQœ=&ù4Îª°ÐkòJ” Óô÷~[Æ0dˆ×NÄ�ÙÐí£�‰.ô¶ÐøO« QTDË¦°ly:´Ú|5‡ðFy
Äuç‰2¤Ä:,ê+ç³w1v4)ïáLŠˆ·ÓGñŒŽ™"]°®CD
©èH‹:[Œƒ ³=;®Z"CíŠCÆ¨ª8Éä—ðLØ9CP†3èö,,&Ômlóÿ— _.Â·›&®3ð^Ûvê¹öÍï¬�Å¤�÷>×¤>×7úwÆ8	Í„¼‚Äâ¶Ër‡ØœñÄÀnt£èíx‚*µ¿þDèTcs÷	ÿ„+X‘ÒØ4o!"ŒK×eØ÷R+ã…Þ60*“wbdjAÝ0‡ðTÉNhðgt±Ã	äâ+ù)¨²6vAæÑ‰TÀ
vöK(9ëmç_ÚßŒî3rLÑ$e€x¤,aPFùÃ±°°Œ„ªGWu¿^Ìt½E^gä‚³òÞDí	$#a…ªQ?sß×â=ÿ'
ØJ«bþãOèyÙÎ÷po79àèñ{¿Æ7x{eyçeðKÙë8ã
²bM7î¡±ûH™“ôœ{§DÙ·ùùÑég,jô’Æ?›¿<OŸo|Q½YËÛŸ/ˆškŠÝ³*ýõ{O‘5õÔ½2ƒ³!ë_€£²
&]¬}‚ä¼	zVóKnM)Ö¢÷š”îò�£È\`HõÔÿ×
§+ÀùV%šyÐçqWP&{`&;"&@|ªõ6Î¨¬õ¡V.`(ÔªÆ2Ø_êäÜŽ[í†öXUW
ºo”BÛ‰KiÀ;àíüC¢·€¥Ÿ“é%#¦Eg¦7_Z…úö•,„°O”,Î_Æ½”9y=-Y§ñuotæ\¸xåÌ)Ì´ËÅAt8Z“á¢(G¼£˜NH¼Ô¥‰ÄCó ˜Ý;Žæ™_ÜÞØµ-ÍÝƒRÆàY~¯Õf‹Ù)ËXq†1¯#4î:OÄ’ß–¤àåýU÷˜.ÞðŒwì¾L.íó˜}ö	*h&I^§K<ä¿?7fyÿõ"Í¸º!‰Ñ„h07z˜:î3Q®õâu…éuÿ´`e#S=µÎùzŽN†ÆÖÀpicµ|‚z¸öøÐhŒ{` LŒR1‰+d*Ä$ðÝlKñL4àÜ÷'êYõ)ò)Ê©ªïoº!²õ~íÝäUº&µè›q}Ôœ¤T˜pwÁKåM`tR÷*ÀtW–&ºE%RM¬¬‰ Ÿþ8áEê~·>ú“ž
´€42ó¹ÇN¡s0	Ô–Â¥„“+rÈÀ˜ îäJ¾Ö<ªîmÔ.0ŽTçOið¤T{âëaþ¿ú°ßfEqª÷î2(Í¨«^œŽŸÁ{Æâ»Ê¹Q$yë%U¬º,z)PU‡Ï ¬”4L«³¥Tn.UQ_ÌX¥³-ùRjÝçS=„MÍÔFâ�+qŽ¹N"[&rÜï¨»:¤É]Ø’Þîº÷«W¶ªÃ§ÿºÀD\ßZSàoà¹	ýÉÌ‘}Yø¨öìY™£dÝháÍ-J˜Å'JQ¯sñCÁÃö²7¼ÉÄ£Ð(Ò
Êy–vsüƒÂN¼³][	>Ñ¢Ç=¡:h”-êŸãxob;_­›JÅ“˜Æû˜PZ4%0^PÒª»
ÿ ÝqÆ^)0%ñè½c¢ÙL‹pÓ“Î˜ÕÒ·.Ä‰,åÓ‡ýb©�öaP01ŠCçõlË$c]|ùbC'DËqÁFèÌJåo÷¿Å?ÐÝÎ�ˆÕ ¦1¸¦–Í=ªŸ+Îï¹ŽìéPä)í‰aNŠä²7ëÓ×Åà½"Ä2;¡LNüÆ—?ñôôªã \Ø5Þ™”ï•ºE^PðQ›.~-÷Y¿^[NQ„£Gh5pÇ�ƒxZòy‰¸nÌÌëcê–l?ºã}Ü‘q ¸G]¶ç@u
G'¯­šýÞµ•Ù¬&wõ.þM‘7•Õd&×ºšÿDK¥é-­0ò°ã{û£”õ>ˆÌf|5­<3E/w5E‹Œ³wæ#ã’1°Ð±“-¬å€™uInàï3½mNø0GòNh°ó¤—QS~u‘Ä]E‹u‹õ_}_ù|ë"&è”½FüpÚÞ�êé‰¸ÊvüåAªŒ{-¢‚Ôi¥N·…3×W–:(°Lþ2hI8>ñÒ S¶ôÌ1
gFßª¨q·%IJÏw±ø TµX—LUä½è+6}¦½É¯ÖªÎ@ì×#`¹ á¦ð3Ì—‚Ú6Ä3kN\$‰&6q$|ÒI²Í:zl”€âWrš¿ÀRmGÒ	ÅÝC³¦Çå†¾Y(E¹Q‡j§æ&¾ ãÈYû`à/Qª¿ë„Û©14û*J¨2¢´G|âhôN='£žòÕÈí\“p˜û{�uÝ¼ì7/ñ^‚<Qú²H'rA+™HA0T…ã]£åq+`1Q‹'Ã¾rÎe]ýØÀÇµôfš_wS†VhA_Yåî÷Fsîmx¯ó¯~�X£0dTÌû‡T¨ùìˆÓ–L=fyU€ÔeŽ1�Ä:¯!Ä„|kCÒ•]"ƒ¬DÂów3ö„gNóÝFÛ;—ðlõ{êé"ìi±=ÃñîÊ7�Ñøô´h•Ve\—ùEOd˜ð.öoF¢XRÂ^ÊfýÐcÀë_ðËi“–UV8ÿÓmE¦ŒÆÌ!Bo;==ß.Ú?·¯w’†Mš7…ÂÂoY
PèoÊê:3&ø5ÀsKbyOpXDÌ8#8:£¹Ót@$Þ„½¤Ð?¤QNÀ¬¿JJ,“‡QÕ)ðÊg´£¬†x6R1’!¡iÈºõÄg¯º¿J.<*.4ºíC…s=ÇªSÝ_F§j«\ÃgËTÝÑŸvmµ¡1Ièã¾7jøÅP½~óÕXúiv±ô¯‚áù,Ùe“%KÆLÉkä
¿<uÀ“O£”ªæ:¢mMÙ"ï|ðƒÒNGÔR®j:ú‰¬oFxc9Ê¯Â~¼žµÈP¦*/àßÍ©Ó†è‡_Ôîñe€cäfn`‰àÚ“á3³RUÕ@ß8¥`õ¤ÊìØO�G;1kq�m6‡´-W!�Èqv ³î›¯nžàƒ-´`¡›HLHFIs$x5
ÿ˜vgíÿÖnhz6z²»ý•o’hNÇ¶»œ÷/×# „h¡g~VŒó‡Ïˆ¥—ÛE4TÝOÅÑs¦Q„²ÏÎ�ñÙáÙN¾«ÑÿºÉ”ð­˜-;W
™Kaúù{€AëÂwu›\éHuaÝh„u{e{þ‘¥hÀ%kïð¨1%‡=ˆ^OËãðÅ`$aÜË@”ÄúÔ%uê‘Ñ([|±Ý[C±ïÔLÇRëŸE»_ÀØô¾KÇcGƒ	£ß\a
íVzø§c¸
üyÂÏÒfye‚!-tb,¤Úsà„¥‘³0íaí¦¯7Ï™¿ÍÆƒ¾§ôl€*Ç¾Í”¡>	Ð©I}@.ýÁaOÒbá>ó}:(µØÂóAÿò‹Xš
ÒtXœxû•unû	’¼3Zó&KjVMu�Dö,´A.R–±¾G›U%G	åLGÓš@¸$^ÇéTÕSÓ­jŠMˆÈ	°Î´_E«y‰íH¸Æ‘ƒ–¤&Qî™ü
ˆfúdÅm¼l”¸_É
±TøÎun]D_›©@`/NŽvœ_
£GÆ:S›` )]Á$U&¿È
ç›©;À®ã\
¡1•)âÍöðNæ¿xc]€f0¤Ã_«íÃÝú·½ýE›¯¯è;£Ú–¨Ïm
	&Ur6NeÜ~:8J|l&ˆ-€ÓÓhPâ±k>™Ÿ¢åt©séA%ÍhlÈ|ð	/ämy£Ú3pô/uH[³øÙ‘»XÛçú@ŠzCàµŽÙKHHRÞiMm³?Ð—Ÿ-©;Z3 zê>gD%Â(ˆYuž4»û=ðÎ$ûÅH—`·F†Ëdtmjñt¯.]›÷–Ü…Žv šoXô, …'E•¨m±¦)DSYî¡|‡*"c´
Ÿ‚Àv?ºáñjOM(ü·e{@ÂÜÎDÀ8oSìý:­H²¯.È¸Tè–ÙŒe~mKÖÃ+DäžBW4W
˜$Z¦cåÝa§ E“ïL{c¹ŒŠŠ×õ{þñ×$Ê­Ò>3sjÊjÉ=—|vhÉÿ’Çˆœv–¥®ˆ0šx—ôÜBé,Ïø~ºéÊ¼õ)óm 	XN‘ó¹ºüÂ'Õ¾ìºôúÉ¸™O;H-"kú¸éñQW<¥º§Èš‰>¸-Ñ‡J˜¤sN|\0_ST	Ðá7j%+Œiéy²‰ wƒQ·ÐèÎqgU…%5'?ñ©ãY mOUú»Ôl‹iŠŸÊ)ƒ„ºS>L.zv×óN-�´#®éúˆší‚½¬t5b"}6teÑEl5âÞQ*ðî?¿ë¤ôæ(Wcö¸©ÕaSkFm³‡H¤!°å~¦Ä¯¾[½ZoØ½NútÆMÖã/"RI~C(WÄÿÜ,È4´û5…¹ã÷1¨2˜ê]äia?ýÍð>ˆ¨J ÝU6Rš|0ôë#rŒ6c<[µ„<Ô³?O¼\‹þcŽ´5•wùP©5b�1[´é[é›%†Õâÿ('¶j L-\»ƒ¦°Å;kt-†]
Q ÌÔê]íåüï±`wž¼ðî0Ù"ÂÔž±þU¦!Ì;ìõ¤èVÐrºÚ¶1Bá³(é‚‰@Üµ&dÁãéËŠ=¬(èfAQ¿es;SZ×pÊ��—#M\®¤ve À¼ßÖ!bØCÌõe²1vÃ×F–“²5*³9?ûÏôð“¹I`" hßÆñ^K›ËUÔF†Ûk
KÒ3ûÌÅúÝ_5†QÝFâ÷žˆédá€àÒ`:à‰Z‹#JR9›ž†Išâ5Ê4R5srõh›ÖHw9.ƒÆ„·{•@`GfóšW¶„s{s`Úû$àM²…ð…ñµdÑI˜>BÑçMÚÀ:qFJ1î:ÿJÃkP`Å™l)j)¬[Å‰ðKÑz7’!^Ýðßž¶Q!ý9h|!„p?•ùÄ«Vgf)`@JÄŠ¯4ŽgºK4‡þOyìQgºD gL­€ð˜ààm}q_È˜bìÜT^lá Ž³ý}Ÿß§!+ãç&Ë rl5à‰xÐH\
°S¢-ä L<†¨r‘L_3jŸ»ÌÝª.°Ó¢\%'·#ç'ûNOµpf€/4êöše¢ÆÝD'kŽ5‡nF1"yapÔï§¹ÕÙÙðJüOÛíu‹
l!v .rÊ´ªÄ­° j¦a‘t=ë•ux†ª?Š#À \­#ùµì§zC)43ûŽÁÁøB„ãfúvô[8xjê`ÓŸ]4»•öMlG|¸¶Ã25 J^€›¬GƒÃg - 4Å®¥!ÿ¥gèr‘u‡Ãf~#bi9â(®A¼Z‡Šþ°xºGØEZÔ1w´·¬ÛÃåd†¹‰|Öp+ŠVÓKaeÏô·ê ÛÅ®þ©µ/Æ¶þ2’î~”£[”jåÀgôY\¤‡êqçŽ£0ã˜®\U9õ_‘íÌÌ§£°õé‹!ÚL¯Û‘¹l.Y8q†\o@Ö»Ð~h£u±3Ñ¹x›4!4,2b°ìNâÉ¹+7ÍÊ~ÙÎrÆ×ð–iZ×!”‚Â…Æ‚ŽÛNí^N9?WX*ù0JyíÔ~ö½÷8¿‰6¤ÔŽN.GÄªõGØ‚*�6
eRl6@—»S“ bsÕHÍra(ïã‚·Ö
¨×jj£Ìš—j(Y®»K ­¹¥@}$g*í°¡®H?+`&Ín´¶½=Gƒ9umVŒB “Ò÷-Ú¡YŽÁd-Õÿ¤ÂÖ®ßÉ6¿V]×$%€œ‚F¨Ç½ž*Aæüƒ‰”én
‰Îˆ‰ÿñÈºŒ£:îŒÜ*”¦WË6oE`´mÖÁLFVN2?ˆ·yF+ÔfŸÓÉG~1¸Ú*NÖþê7qbæXžlq/×ÆÖã@ØÓÙÑ¸pPMõUÍ‚¬±Íòs²ZØ0›ÞîôÞžùê‘¦ÑF`ÀXýnEü²íw“´uÞ|8-•äê3K>AÎwÃeÑµ#»VUHÚU½˜—gµ4B^eT&Š`k—¿žÿD­áy-â(� ˆtÔŽKo¸2^³Þ3Ž~6àgêë¨‰šì"t1VvNÂ6özIivÏ?tår`ÊÚgHOõ,Eolè¦`3xì2
óÜòš{’0+O½¯úêäwMØûJÊ´´a“)¡rÜOI‹‚B“y½€gžg{J¥2‚Ú0F‡VvøÍ6ªjP¯™ñíÏLßr?>S¹êÄIePq×s5¿} C§‡ìä#L
=ÿ
sÈÁSéœµK#¼FA‰Fgw-^þvúKuçl¨N—
zh´cõÿ¾e¶¡\}Ô¸Ú~TyãÛ±Þ“RºÀ<ˆ™°¥r,(Óz²?¤ÍíünR”zldçÍØ›
°c^šóCßBIÆa/ÈRl:äë°"ËœÊž_W2¦CµðCGtÚvÃ’~o`7h6ˆš¤ÖÈÜ]ÚÅ$«5À"ÿÉ%<‚¨Í’od\;ás–<c}Z+ žÆ
9·”é;m&B¿àåMîáx¼ÁgÊl3{=%üïª#ÝiþÂÍ÷T$9ç;ï*„_0!žoŸßbz(b8dÅ¹{¬Ü—•ÉI\h±™P‘U©Þ,¥þ“®C€ñÅñd¯nSD(eem¹ÙD�âd6êáãŽòíœór2e”ÛíÅæa\øê.|&c0Më«†EªçÀµÝ7ãÕy>ûÁE°š>?^Çð%Ío,âáÐ¥ÆYCì/]‚'˜¢†<	p®�´¥|Iª#›°§ýÀ†x–­j°Š*ÞSç~…øQØ–b!…Õ¥<%á\KyÂª¯ÊÃ&aK(QçÕ´¹)ºÉ‡lˆHiØÐ…ËEzk4ö3\9€2Ž×÷[¯<]q|ü—~ÛZ•cÒe{ÉdÚÈGIÖÖð)½eÔÀ*†
µÇ˜:0zNMUÖñ!º#éêy.·ér‰_$*�¼QJ‚@Î/lŠÄñ²å€ÅŽJPÐŒHÛ5ž´¯â«ç-ò¯«×.ƒÑnÊGô ù
Àì¹X–þ�ç"_ûGU,½f¿b•ÞñOÍ(LÒë„åýÀ¾ˆMSa!Sb«žñ~ð¶`Ó€—?Õ±êe	ëÝƒÒY˜Í¬	†ŸdQh<5å+–ü`1½	Ë‘Â41‘ÙZ	R·Qý¶¿mœØ`ú^5Á×mK7²ìOlE8L®Ý‡âåúà]ÂŠ¡È_ï¥¤¬d¹ïvlYxN27‡Ánd‹,ÄŠŒ›K`„¥éÝð\PmÍÑÜ
»°Ê&uTÖ3ÐHLŠ¶(,JBHZñÍþÀ²ŽÕ+X>Ê"°¢PµÓÁŸ+¸kRùù
’¦”#W¢WÏçÔÄÞ¤+ú§5ÏÚ]@ÈW,‚[Ô¹Õ"pdhúX¾@ðèÜ]ª¤Lû–/®— CoÐî‰ýú÷ó0ŽÈI8Kël'ë­nåÚg¬·‡zœ•ÜÐ×x„¤ÖÎ(h0B¨©k
Y3ŽÑj¶÷g.ôèÚ'‚ Ià¥ÖIy@_7{ãôA‰PœCœæò¦ÓÏrYäJ!Ør>¥ÿ¢„P.ä¼òXÏø«‘<ÎnU0ÜÁà†/ZëÔì%$‹2Þ<´sp£úqg¦æ’hìÜÙ4n[ûFâJìü.¤È�Ö}OÍhŠ|q‹¸É,d×:§_^X�-"vÌÏ” t&90I)}nÚÂ(˜:Y�çL½Ç1ZŒ[N¤,S„l›RLÁ”†L²[÷-þ¼.¹D„HFU°`ÃŠ—?Ú2‡¢±6[î!,ó žàcZñ7)É‡êÔã³Öf©£Ü¹‚“*XÁß‰3ý+ý¬ n)²a›„G¯]Å¶ñ�!¶9cù5¨.´ÆîZ1û¶ó·!znçãt9ŠËg2àM¸Jc¼ñÆÉeW;ÿzBë÷Üéuµ…çjkã¥E+sêb—˜;“óÈ0Šå¦Í2SÐ¶Õ-¬4Avdî	2¡ î«ð”0X�`ed‹gÝ_ì3æhá�u€û¼Ùµ9Œ¦.“˜ñ9
ëpa®5½ø—ö±ã¸‚Ge!ñmjKçg`ëJ…OÙöøö<³µžr§Ÿ`G3šßT™–ùq‰"gA$ßÞrÍ–ÝÿO*8D�~<Ù¡?¶”í©rVõ±¬óaóÅŽ^lÿ1)²ÁYÄ*…ÿS·Mu©ûïQÿ‡ô6Ÿí#–S¬/«Ÿ�À«äN�ú8LÔ¨‘ÉY©8Ó³:ëÞ~müÄ’P˜õZ
p¢ù\Épö0‰Ý–6÷éådüüŸÓ=ÔŠ3"œcWþ÷Uü	zæðøkŸH"fHüøu¹h«“É Ðb‹ãâ/~ÏéøøÏòµR=íÚÿY4R§Ê·ÞS]ý “Cb‰Ã�µ¥�óê–©&›³Ð›Ó¹È:©zÆ”#Yª­Å¹B­T¤th¬;¸}õá}È8[çÆ1žƒ`*Ãˆ¬R©Ð
¬/–o¾6ü!+çå=ßéEãƒðef^°pŒ¾ŽkA>‰ÔVc”�á-öÔ2+îº¬fÑ›l^VõèX„ÑFk4ðô÷À†ëð:T€´„BŽF(‹Þ*ü€@2>3Ô²²û9à™›vûÒ“ÔÐ<ÃBï':Ck*— l6mzþ¼W¶çø^YÆ¦ÙæEÖ‹’ØTq·ö}§ýÓ.)W‚	ø<Lë£ŒýÒ¥" Éöî$.~”CØ`3Lëæ¬ÛÔí"Åo­ióVv^|¸Œm;³RNÓ(Ø†þ²ßõ¹úd©r/äÜb‹l4/ÿ .g])”{´êÔ‚0Úº0–03<{³xiâíQ±Öxã©ˆ/g{¯hõÿ ±fK‹ðù>ü\Íæx*Úpk/“7Å|ŽêŸ8vÄ¼ä.e&y´`\&ÀŽšp‘@pé×8ŠXä!:ËàŽgš$µâMÿU‰—K³c›K%XXêôù("Ì`\Éâ	úðhrø3ºEu^·!ðçÁêášdA¬Æc`[=Øž®BÞy¹(úcÿ\œé¾ÙŠS «’<$_±Š´sõrfÄ?£q©H2*š{Âp/„´$m”¡¹•¡Æã›
=ˆÝ=¢ûí^f±—eŠ`É¾­H|V+KÙ#ºŠQÈ=�µ¡øý˜Ûo)ÉûO‘¸&<
ÉüC ™1Í¿>æbÎŒiÁÕ†‡Y&ùÒÕ®€š*Èã~8¸dô‡s7è>°pßJÙ¯Q0)íF÷Ì˜&ô"B×êÄ]‡Ä×ûÐ€.M'sã]9dÈxA³Mz{�èÅ±Ó·O
Þôxm«ä‹#w[<(÷ÙE=¶¿XsiX19â
=9%6ûî°ü…Y~%÷a2VNµë¿€ÆÉ.PîºŸÇMšê'*¥jKlÆå¶(%ŠûÔNÂ´�·­‘óR’wÕ¢}™á%œ”¢víŸyOðµ,u£mw]øCZ}\ªÊÖ<µ;ëÅ†ª©¡XKë­ß}ÝÀ6q´ntÙ—QW¼_ÃX•cCó€2aƒM´A_-qÊ|�l¿€Å®ÂÁßé!ßie³òq¥Ý×ëG~y¢ú{yÐgªUÂì¤L=‹à:0ü0Üü“RcÜÞ~«(Ÿlk=ÐE)<àÕfQöž¿Wtð9âDë¶ðfœ›û)äkJæÈ<x4v<­Ø°2Eþ5omµ‘®.—â?0Æ†¼¤C599œê·‘ö‹üÉ”gë˜þ"JOG4}üô„]œ®T]E�¹@¢¥cKØ	™Í’¯F[¿[6Et¾sãÓµ‡oúß�Þ\³°Ý}Ì9ºi„t½RöLë5™‘|\M{ëÆ¨þC¢ô‡mý¨ŽÜ’lÃÁ§e’0}X—©8ï¶ÂÔûÆS—sÍW`CMh âóÿ$ÉöÃÉnùÜ‹„yOUéN{eqMßÔYÔJÙó Äw#jGb…n‡¬*{ªhJ¡Ì×)txrw}j¹yïÅ¼ìî+wÈvñ9ËÐ2ÞØÞÝ$Y	šâ£™JœLcÇ¦¥rC­@OÓ6sš`ÇÖ¯ÿ‡)Ä)ÖØWÆb°—«âpG±+îaŽtF·{U³H‘Œl¤tæVÝ+Ê¼HA¤µüN,]´ÎÑP2ñ*+’ÇŽmúVDmæø¾£|w}6QŽŒKˆ<ØCJsˆ®šião{§=	×Fqr¥Þ"ysp„+³>¨P—^çOáp––Ò“Çü	QQc]ƒ'qëB"q¶ñ	L´hEû7pIrû%ò/­„^È‚W#µ¡’uøµª´óZæúìl…áe.ãÊsó$8rm˜^›î2ëðÃR©Nyç´õçuÇä)Üä:õî£
ôÃ``´'±š­ùA²BÓÙIœ%á
‚£ÿ•Å7Ù×�fÁJ°ïÏäÀóoæ¼9„FÿPðp=0·SöH!9_ÉKˆ²D›Ë2uÝ‚õ~ñH²Ê¶Jydë J¢jTf>q×G’¡å°Z»[3¶Ñö¥ƒ4)¶Ô¯‚¹M…<-/Ï {´rÞ‘_mÑx±Ãäwè%jçËz!.
’è(7þ~QÈßöDÑ'²#ÏS ðÛÑ*Ö¢±èr‘t‚‚]1“•J×ÅÑwúEµb-,¯ø¨Ið…Ù$= áûÁ0èb
e(ý1V®«@VX£èÝ—LÀ1M¶Å›øÍtùû©£6L©ï[bîù¦ÓfNÀƒèùs&Ênƒžk¶˜{oÌ1ÏÉ@æ6dJªË,,Àv;k‰¿,‰S§çZ¿˜2ll'—pêœ?Ã`èúÆØhÚMk÷ç'ÁtŠöµìÿYTNäÄ¤4Ýß•áÅÖ'íï¹í™hî_®×D1e“ˆ†0ûI‰N%XEíxuH»¢Ã¥vfel"ÃÚÏåw¦ÔŒÐ (GÇÿÂŠÃY­™ÌðÈeš.²Ø,ˆÔ“»O¥ŒG€Æ}½TTXh
hšïð‰í'÷»P!iAÔV‘ýà¸ÎY8+³;
ìi‰¸i‚¶-ˆ´Û^p6ïê®ðp`Š¯Ý„dÿ;«³,±‘W<-•Å-he@ö<¢nÖ«*º>‡S|R‘ïj"÷8\¿”G¬ÓvR‰6YäÇÁ‡°ò¥x£/ÿ©ÕaÃÆ–°fW7‹•6ÏÊ<hœŠžÛu«Ý(Ä¥xp±B™~Ø°Ý§§Ç¡r­ù°i&cH¾Ì‚3q+¼*u õ èi‘ê
ŽÊgKGîºuàK€|Á­eâàH6Õ«ÆÈ¶œ£Ìkå"ÜõÉñqÂ>ÈÔ¬ýQ!0"ª.ì1ji"§Î¥Á¬–9öoú&Ì”c–Öd<ÖlìMŸ.fÿûTB0¾ñ1g4É^e‰›)"úMPZµ„ÄÐ•¸þbÉî±š×Øqx;¨kã‘N±DãÔ¥~Åv¾ê$RRƒ2;-žsê¸‹ã*y‚ºú‡|“6$n—-.•hxÈ2–«e7xu@ùÔ?�8Ñ¾ê0äËwUÎzvº÷Ko\ÞàOëóíuüì<O}áMá¦#nà‚wiÞX+ótßç‹±nò‹“™•žüªÜÌ+Ò$hüGE¿‡(¨ëåá$EJªNl›3ÒEåvübZiîø<iG$J9lÎ+U¬q[Ã¬>‡½hUïQ?‰“&‡‰ýÁþümu1{é„ò#êjô]‡„ÛÐ5œÓX"ÆÑ	¦rd„!Í±îðä3òÄ‘Vo{\ø›nùÞU
Ïh•80îù¿»ŠÑSgÂêÕîCæ›R^*9¹ÊI
`¬8·}¸Êwêªg<“MLÏò†\>diÖÍØë=½°†íž.ÃÏ7•‡ë‡.s¨†GMZÑï(Îï¡ë4%4È…ÉÝ‘VåÕÃo`y3‘ÉlFõÚ–ÿiY™(îÁz…ðÝ°yz\fÝkæ’åÜÑnrô@±2{,Íƒ7¸ÿÆÂ+çü È„M)Ï›ÝIÉ_®Öó®¡Ë#$q–NR{§mÁˆvgL‡lN(ù·'pÒ‹/ˆè7×´€·ŽoT£N¦¼20uÆKÜHóŠÅU¯|–ËßLâÿ´xÍñïâ9®JÒB½Î¢-’zÏ1ãÇs(`«—0boõö'øž•@Â9–7«	5¡xÛóô™YK¿é`á¡
/Æ´t¯·:ÌÎ–%xž"E`+¼$ÞNÎ]
`‡¸GÏùìÙÎµ9l"Þß«j1ù¶Ž«ŠžŒhS#~2!‰ShIbÝ ƒt2K%Õùþ‘gT²e�ñ¼~±Æ?t	¼ÚºÙ¿Ù*ot.áŒ%×Q´€wa¢Šë6F.#]¬Ä··6ÞŠyç¹ªRszƒMèK{,lØý—ü8ë×ßS™Ý>;¹cØ¼a(FkééÈ9Ïm»w'¬HRyŒ¥øÀú¿7àn±Dó€õ WÝ#;œŽDØÈLW)të'd�ŒRZ(øÝ¶þêêóK$0zß÷~ý7w^]^9ð^Å…×J83	—øãÒ	)Jšî—i[B8ð¼pnŸ¨´ÐY„ ãõ(õ£¼$œ¦—#Ôi.Ò6¡EÚ G/\:O†‚±ð×3­w4t@8ElÉÆ,E¶�\!Š)T6m‰À?›sªC!7é|U‘äçA´D|cH¬�"WþºÓAƒè<*K­)ü*­I`ââP@ó0‹Š<}©­5ã¥¤Z4pòXš¸8™£J\SnÁ6=y™ßq˜ñ@oà¾Ï±^9X¸P=Äcï•x5aãŠiaL©Ì#æ§¾Šv×fY­Ò&¢²·ëX—HŽÆevÍÕ1æÕë	Pe½j´¿Á$ãt0øÇÎü>[Oé.}oì/	ì—¥wþ‹yèm, ×‘ŽEs½Œ}ÉÓVC§¼5Ù!ÒàÞ°äcF'ºµèsåÃ?Ñ—aØ·:gWGñJŽäóÜ"¢;,°õuÛÎÎ€Îw)
ewŽ=7ðÿw¥æÍ(×Î7VI(®{»ô{ë
,µ>x�áaÈÅ2Ÿ’=å‚sìoYÜº…s°Þ&0ŒÖ?Ž<ð¡¥øY~ÇYj§®AžÌÈk†|B¿‡¦­¢_, âÎŠ—ì´=Aj6í/ëÚÌµË
¹O?Ð)Ç~�UÒI(v`”QHíöï*§wð6¬Üv îÒî±f | KˆŒp¤Ý–¦ð¶Ì@%¨yðŽî>Š_^-ŽÏiáGŒlÔ\Ú8-Ã‰ 8ý2?Ý	Tq÷ðÇ_ø¢ÎÂtíõ9H6Z90FÃ9ê}šã„ÿ
UN8s´ü¤;½>p-
´ XåC©Iˆq}ágPÍ¸¶ô˜$ñŒ¬É¦õk*»Ò
û>á’&¸é[Ñj*0<+lïÉùiÀÑW€ÿ§w¥µÀòæzý›”XþÞB`qÊqy
D–y‡Ûv¾#R)53òü4!Õ)•×LG·DÆ¶Ÿ4_#ŸÐIn(›qôáù"ä›'£ø—æ®
Kw+îü@_¢üp$ÅñM¸ý	6¾LLuÊÓ43°+ÅŠ*GŠÕK›²ÓÒ5ÌÅW)Ó›‰Š±š4Õ•ü÷û©Õ¦¹›×¿B4ô½Xø@$V:pùÏr-Jß(ÓssÜá´c[p¥À•ªÈ
2õçë— rD¯ „E+®ûhîëÁ´t7‡ŠžQÁCJŸù‰ˆŽñ¨kfË0ª<5I-v‚Ã1f¥0Vz¨T¶K[‰”Ÿ?mtªƒ2ßûC’îDRÎþqT§#`Õk3°¡i<á—˜ulÖV{Štýñ?O°é¦néå×nÁð•Û±DR%ŒR;‹¦è±yŸ9zQø™xKÃ—i½'‡¦6ZˆÞAÿ€‡é³‚Üš‡¥Î]Œ‰E
Ãª3¾¿!JÃªïJ/[ˆŠÇÈ¼x<KEÄñÁ7Øþ±¨±u‚?ˆÍñ¦êNhŸGwäOûû®-*Òïjü¹ãG%û¾~(Ý4éù½@âE7G[º‹”ñcl.21A‹³rLü}¶y˜:Nªœs§»³sA".ËÑµI<ê£%™ÿºÊŸ†åÚŠ£	GÌÞZdÃúâT'Y|tvZ´õâ.´„¾s¬VIN1<*N"¥­kƒR$î-üœ”±®I‹h5ð*¡‡JI³gêäï“>Go +9ôù–h{éLø„L>:›U„ØOÁE™@~Ø¯±Y†º[x &ñ› PnË®}&A•“\¯ÎêÄ‚ÜÀ)!÷TF¸Þß¥Õùo÷Y³æ¶‡»ÿ ~‰ø]D—àyÑúTád¦PFLO¬+2Âzµî‰I¾ä»ÂúS îS–Npˆ†Tí‘É…ù.Bæ\…Î¼îkÓöæ§; +/Ž›åYü5gÜÅÍOõÜ©ÞÆì¿¾ù§µ0y!á+
äÆ6ðNÙx åüÄù¿Á’ÕwÓ—)hfN˜NfÍŒ¿yÑëƒ…­%
u#-$LÀi«”ÝÁwjÂùL·ûr$dt)àãaÄ¡±€ô€‰&4¹kÌÈC|‚˜Ù¿ÊB~òƒ²8ì‹ìÇpÉ¾7ÞÁÕÙ˜ëô;!X± fÊ³…dïö_ì(¤Q9©£tAžöã‘\Õ�Í•é¬+a™o¿àsÂ,
íšlã©2£œ¥ùw#zÈ¤ÍTN•Ày´G{õÀ®¢P²�¿Ÿ˜ðšþ¬¢Ù-’ràK;YžˆNi6Â×´•�’#¥
úµìHb^¯€ÚfŠÒ³Õ~ílM¯/¥-vâ?ÁˆÙeT£1ßOdÜeÝý«À3f¯ùÊJ+˜
ÁQ^¹;=�ÃÐÝôg‚ôÛ‹(»Bd½LÖsÊì³E|jž\*No£ŒˆòB´ŸŠ5ë9]dTû+CëÿÜx¸º'­ìÎÅbç– å·x <(lÑ×»p¦Å¬§YÐ-:\³¡ö¹þÏñ<ÿùœ=5¯)l_X]¡Ž}`ŸM¢óGaÁvö6lŒÿ)aÓAà4ô‡Ð§	¢ú\c8yÝÇÇÅ¯ëþ0Œb^?ÊunÜ“e~¶DšÉQp=•M09
»˜O×KWÉHÖ¹ßóƒ“ì»Œ×6f¦’!±2-–kTÝ“Üøè[à°¡Y’
‡·]ñŽÕG^O60|‚–á~ÞÊå€ˆì5æÇ@ òåV!Š?5ªý´¦<¡ã1_ûþÎŠ±u|Sñ±…§êÄ3D›¢¼£õcr(]ý¥&›ˆÃV˜>i‡.@óêÁhýÌ›%n¶�µ¬<IAÔMã6W([‡:zœê°¦2t
”(Ã­&)€³°C×5õÜäÂ„÷ünÌiMçþý‹9Æã]¾×€¡3¨Í:¸i%l¬HÇÏÑûOKz„ô*åRMô›Uªuš¾Ææÿ)IjÌú)Suûèæ¼`…
u.¥i`ßùpêbSb®õÎ*ñw\Ä0¹ã(%ñÏ9ÿ1ÜÐÖIZÞR¢0ýýj#ú~˜u­qJÇ½¡‘XqŒ‰*Z2;ï·g|ú¬èP#"ÿýKÕêŒöMõåxÈ—²9÷XãØçÔôCQþÚ“g³´ª ¢µ>¯ºæ}Ë¯tš0’’6¿D—›üpô_
³;ºòáÎë}Uør¨"Dø`d1L¹]yvsÿÿ‰k©çáp8µsçv/4äe.DèÞ›NÀ±TœÁr¢QÜñò„¼b÷1»cb»=ë€m²Hû(ç{#$@eEý²ÇÏ
!Ž€Y€Ÿ<aOøƒC˜våì¤BW‡q…}­¾!}ŸB‡ù+%‘·5J©\øñ}r„³5fÔþN’–“ÊÛ½uýÍ}™ñ±ª1I	Å‹´|ý¿÷âLDêÝ‹°¤.‡>µòsúòÚl%ÝÝQZÇ
ßœƒøâë«B‚ÐÖnX
=É¶êLjb4Ô5ÉªŠ3G¼]�fÄïý¸•Êtu8·=|,¶÷õÏ
üÅìÊþF›_žƒSö°´Ð/µ*]á8º™Jà|Y8ã`M¯Ûë¡ôXwºl°ã‘–Ø¿-Eûg'\\Rü÷
ÀGSm®û½,£\ñ2Q)yŠÜÇó<)|<n÷.ßº(;ÐìÕçÒV³˜³ôáòl>«mÞ…¸qöÏ¢‡ÓèEg\œŽëmÆœ€­¿¤ŒDÒÜÓ&m”VÉIg³îäÖEÄ)Û³Ë$®ðV±G/·Ì¥½ô4ÚZÄZRCt£:ÅÙã¯˜í­T‹ñ²nÙ3Zï&ì.º‹Q½§ëÛTÑ±p3­2ºoe˜†­…ë¶g¸n‰ãÜ;(„V=ý½Þ
{^O40ö_³_wã„«$(NVRš0=ÂJ}³’?Œ›ˆ5LPn†ÙBa
Uƒ¼k>÷Û„‹%à¯ÊÖú$åòŠ'($9v 7°Yg}%ù±+?=Ý£äZ-£6<Ä¯ê6ƒƒ'3'*ì1~ž‰½$)	ÕàlG~™ÏÑÓ1Ñhž³ÌÐ>GÓÀ
ßÙ;âí`’Äå¥l@}ãÓS4æX"bçCæ{ljXÅk-¿:@5/Ù>uº>`["Jäl! F"4ôò¸Ù7Îæ‡C²¶7 ®ñ/œÎ?Ï/‡dçýLÉŽLèC.ÓÞúý[ÒãO|5ãÐ0ÎÞb¼Uã£GqÌ›Øí¹|ì×Ä“6xD	EF­aµýä\›}Â7£ìq"mzè&ðÌhBIÈäyÍ÷M¿ :Ýé¸’‘(¾ù×gfäëÈêønö�ˆ“"L½?>«Õù£q,'Þ¤×Gƒ™êUj„ÌÔ¬qAàµ±1„Æ¼ÅÍ…Í¤ÝöÆ
O{K¸µˆƒñÓ•ýåÿ;®®£
Bò¨#DÌà6Ü8ËG¾� É:Ñ¬¶L‚˜®EËgÐ}½§ü'´ßÃ4ÛÙÒÆ±lw|ò;šÆŽ|Ÿ+ÎyÒåÍºWïÍ-‡ŽÎžFè”–’wr©Cô§¹¢1
W¢¦+ü›] ì3„ƒ+ã{Ž/¥§ù_b{¼[	¸ œø…LF´&¸¡ý?Hö0þo^>ßÝ,ûQ|jmí>ÈØO†Ãp·ãjí<·µÚ8ÐÐ§ÔÄ%)%IH›T³f—P÷hmõEº”§¥¾¯{rÔÃôÚ­k
ŽI£Ü${b$Zí+4Jòâªc�jçdá%t<Ø°(z™B{ôb×¸{ý±…V„Ó+×Ý\òBÅji°èÞå´ÇÊµe¸Ýdèàx \!uÌlcïüwl™Qh³c‹çør41k‹'[M“ JM…¨s µRƒB™	þög2�õ;ŒTaê^hºn4SˆWÈSˆxßYƒ¾Zb½5ßÎæNqÈ68º´C ±Ó4,ÜÌ#´-Ç.ÌüÐ'c€âéïˆenû¢_¤´ô‘·*¡'r
\Å'm7)«Û±ø¸€øÑ/Qçê+pè,lä”Xù‘Ä‡M šÉáAa‚ï‚ŸD(Öåh5ŒºGñÑ{Ò±öl,U­.5ÖFëR&õm…ÎôT?!‰~#½8V~…æí„Î´:ÖÃÔSæî}ÑÏ	s4’§³ké6ºd©ÐgÍºº*oág¢^nšS~Ü¬ÊcÇEÕÍ>§Öº–áëÈB4L•Í1D8;üf0xŸ6WBˆÅ=!Ÿ7ˆW”^wÊ‚tCZýúþWß°ŒãABd~õŒì õò+.}?ú—Wãu´Ë­C„WÍÿöj²?1™ô|ùmÞ–JÃÓJïÆ!mä¶D“£C‡å.©0Ó/ß\Ó1áy2]o
ãõü[8¯\d7bëE>Ç»’Ùº‹¬2°põ
ó<Eø£D¦MVÒoÒßa§›O*¡`e–Z·àÎF%ZC8åIÇÖÓ$’ÛO‘ç]IérýÆ¦]+à·){÷ƒôÄy&—¦Hðûù´¾D"kË¢àˆxÈTä;Î…©,@S‚»›|ÌóÚ«–?	û.ïSØäïušiÐ5tü}¦r\B‹±]Éú™;*Í*ELqÞ2T>6q$jÍ×ÀðÂçü®K”o(=ßèb€-×*x"|ogküdg+_‰g[w}™ÿS¬)$ã“‚8Ä€¹Ó«ƒØ-©„Ç!}¦<ÊÒÂ“yGÒ©¶1ÙziË+¿¨i-i)¹Íq¬À$zø) ïûž>,ÓŽ‚]hÄp^QkËšÆ¢eþ È‹ºrÆ+šÿŠôz›÷æÿÓ«þÒ‘ÎíGž+9	n•“BtÛÿß²ÂeuíàÚA¢bþØÈìã}?bÍ?sÃOúqüÜPÝÒåx9u~‚]==‘FŒ sMíÈÿÑ±¸ð…ìÔ¾“$xñòuÅ3¿Gýµžk·B;—i-+=ë!N]Xå}Œr=G
l’	6ÃBÃÔ²3eûZÈ1ßÁ°säÇl\tï2 3"ô21@nÜ„Ú¶Åü(:ð0ôä’9´„›Og¾À€‰:»ÿ±Cu>vñûéif‹OrØQ—¡$ŸŒÿ±DÉ!Z5¬½µëï€',˜6ÊÁ7ð_•0µŸ.¬\úÙî¯=tõpš¼–ŒÕBƒ.¢­o@š''C?}Y˜T9rÝN¶›²~æÿ˜…¼ÐôFû˜Œ ¥4ùì‰ÿãjõb!gWzl¹CI$Ðäõ™*ìÆW>iê(9[e`µœ˜÷søB`Øo€—HÌQýí:²)Î¨u”æ¬g¡LŒŸ#S^ %ŒehGnˆçÌ	ÿUDX§§= ’µxGõ¡f“ˆŒjgn‚†ígÒ<äe=óX€¬Í]!Á<.Ëb+*!WZÿóz›ãŽ%õÜéÇ·~¸»‡t³kë±·µó_Â>a¡o
ŠTäüfE+ç¤|SåHGOgî•ˆƒ’~@°÷âPŽÃ»?�QU„ÕŸGÐ|qØà=.’ùˆ¶‘—T~]¸N©ÍÛ^ë0—~‹·Ç8ŸñiºWYð]O¦œk»–i+Ð…37
PB�èÌ'dàÑRòto
‘Ì/@V52Pc–Õ¼QŒ(ÖÝ'JÎ%×v}Þ_YJ$Äüq
nâó„ÛÉ¢™ägMÜåþ#žAmíËÝˆ;?Ú<`AÈ”Ðlì&›[Ÿ{‡0Šâ8ÔÐÃXøæ~Çðî®1ã™1X?V3íG{¥¿¸²½Fõƒ¶_¹«&wš 8T}Ê”¡¢ÿûÆÂnô`ÞvïòŸ_fšØü°X”2flw Å9OŠ9 ÿåV·Ú`åŸÙú —åúD¿@ÑWR˜/´ùù°=·zÕÕP¬ü•~|dmu¥âš‘%f¥i
†VÀ¢)lUñ ñ
ª]¨çðwyn]*ám0K»f@”å�(ËHým˜,ö¹W2æuÛ$WÜ3‹îöŽdŠ21Œ‚¯çKèŠ"!þí[¤cäY]i'óä¿ñPïóOÓ‰_gšžÑnÄA•í.«7p4!¦¤H¾[#E–hIOk°=T¥éi½öJªOåõ‰ñ´€"ðŒ²=L<ù.T®éƒxaä’ŽN“²Øî‡—ÏÎfz,)Äoo<(ç£E•O¢6x¦WT:ã-*m~,ÂA£G¬FØ·6ÿqR$j­N$%†šàUó}ìuƒL-8¶;Â’[5QTÿÍU”f´£ŽWÄ$å–Ò4ÍèÉe`ý÷÷�Ÿ­ƒÚé^ìT/µ£ÈNë„•¦ÓÑwBáÒ(-tìê;çãÿ}˜¤Èà@ê²na,º¾×)Ó›@¯íÊÊšÆhÆ/‰q¼‹B˜Ö\vN,ÑžðÇW›Àåq2k±ÎäOMÈŽJøHI¬Š½?l¤0}ùr£L®k„0ûžÑc¿‡Ø,jä€/)ƒ+«Ì48Þ¸Já
H�?1««'>ºÉé³L_IÓò>¡r@¢G‚áÉpŸ¬‚<åš"Á67O?‹pkàš`#²®ðÀ{ÓÒÐ·´’[R9ù¯²Ô°‡Ù)Ì‚²€°/W¼÷Å#üpRW•Dt¯F,1øëc?4»VwJ°S;–˜è&Ú‡zÊÝjÇ®žbó&q­,ÔßÈql“‚)
‚;È,æ�Â¿MˆälBI8º‚ýƒ.¤§‹‰Z§¨¶’•+Ît
ï…6%ÛL]iç­Ûä,1Ð#tß”ùh]>=%z^hðLz$îƒn5„¡F§ËuCablKŽtöó^A=‚nµT*¯¸ý‹X´Iº“çÒ hA•Ðú#Ñè‡¼˜Y;¢Ä@‰9—‚Ì¸VÔÑ#© -à*r3‡ç6à jCTs§`Y÷†szAò@½ 
*³ßßK„¸Ýv\ÁÒ†–;;â#A‡_3ÓÌƒZ÷}!q\ŒÁ)¿&n\è§ÕÉø2ÚrtAæ@öl™ö)(!¾_é•Ú¢[o)Jù`«CÊ¿z¬uüÑÎâs!&¡e-rÒ äÔ¼:C‚S­½ÍàåƒÊñ÷MÆ~]—Î¦—ps´2˜!Æƒ|(ï¢ê\\6ÃÆYqŒi.É^¼âE'P:K ÜÑcrŒ}={:î~¼Õ
o5ûq60ƒ®%×{yýÞJNÒ#W`„þ„ñŸ•¨‰V&Ù~ûÆ,a6Qz ßøúØ¦ì _H”¤Ø7e;(rR_÷}Òñ‹”[>óÐ¿t[]ÕYÇš*{l|/ø:f<ÄƒÕEo£êCŽC
‡×ðe“wásËóõó×ÉRÈ‡[Æ¡ãVèÎ½¤Ëjà{?xÇúOúêÚØÓx®O³	µJ¢Io½"†Ù.RÀ¬ågœßxãØ— ¯L¨Ì1£õ|Ü�EÖýnÔù™c`sðÝgùâ�¬¡ÞDgÏ—ÞúˆÅ3sþ6¢úSTÅ¶‹=‚áH:ÙÓí©Å(¼d Áéšœ‘ï&A:<x’§½ÂPc¡é%êK·Ò¹t¾rc«"pôzÏ•ðH5Ç³þá‡]§µn«±~7HÑÓÒEçœaG‘7’ÒÝ’rS™±c,ÀR§Ì´²Ñt÷Ûgp”?˜p^£{Í!'{
uÎ( ‘\¸sXÒR~ØÏî(Z”mÔ£‘xp‡mÞy2cm³~Ë`¾ö®¡'C¹Éÿ¹™÷Ó¦bM“Öa!’ÒŽØœ
ÁvóÐipÈ¢÷µ>š·&ñÌ¨ ê¯n¤"(bñ44‹F&í©-uRg¢B·ˆlgãKJRtea²®ð:d™S,=U}Æêˆa~Ãue�nëãÈMw*Y{-¢Í]¢
‰¾LF«y‹:¼l}à(T6!»ÂK~3	ˆk®Çd*wF­ÂDÄK´v¦öi¾—[OgÉˆh—ØÞŽb-®Þ‹þ[U ²¶½çí,ªÛï×“©Pîã÷²(³Ã"`¤®!®ìjÀÒÄþ¯¢‹hÉÔÁ¬^iK‘EÐÆm°h'RâÁÚ%ŸyeÃ¾l&ÜAw·ih«ÓÕ®  ôVœ-áéH~¶y»«g%ÛmµÇbªG ®1\÷¨=ê‚q§–ê
IþOd$¸#K7Œ7”7£k¼˜rÜ¡#DrW¼óN…¬‚;…Q-×H‡nº!¬xL½ø‹B¡‹¨•jH
ÔÎgôä-zu^«ùfSçX2.™2å¿µ^l®²BÑ¼ú³$|Ü5º2¼ýEPyy.nË0Eß6ßx(?yÖ’çv.”cL`Ÿé;É±>óX‚($¡QK´´0ÛJÜù÷ŠMèÀ‹º<ÞyQ½ŒÆõ‚e&‰±«Ùî3	4†Î@\¼^ä?®ª	ù?JsjËD¾˜`R#)–~h;†f–C2_…\}Ø,!ÌEðçÉ9ëü@ë´öâpWO·¾Zn3%±ÖàÜahðã4Õ(ë"µ=¿8ÆXk[Ñ€ß8â`þÊá…Õ¸¾1„dWVèe08Š)1¸…wBû~9Ê?”©·Ø*!R·ê¥feZêÆ¹Ð¾÷W™Wþcê
)ÖäÝã>áß¶&ãzŸ�-ÿ¬yJÐ@iùüþ1MÈóÅ|�p¡è!nVó’Æ$TˆŽœÙˆsFŸÅÍ²Ð`Þ(·,¶wk/þ±˜Ú.\ç÷ýÛ¨–“)ƒAíÉï7ªOïÜØfDƒŒ%÷Y)ýž
Vª9šË×¶¡wõ
Ð´ÉD)øG)Âæø¶#¯lôýÀCê¨¸4ºHY!*Õp;?èÌßÚWÆÀfÌß[ßÛ»Ìâ•ðIŠÈÿfù½î?øvsÍK¥6+»jRëÏ7Õ¹’s“‹áÕÍ43â¦Ô¿#"
±ë ³lODaøŽ
.ù– |UÒŠzu¢Æë¹â)E/_˜ßmÿ&1HÙÌLÉ¶6—D‡V&x$4#7^óëýê†+)uý±>‡©|'ä‰GÄÁâÃÅénëNäïM¾bhB½ø²˜#?<¨çâ¿DªGÿ=ÒN°9\Àk‚AL-›¨·k5x/ÛÂÎ÷1££Ï»Òžn}C+tDãÄo¶v˜ëÌ:pÎ á•Þ¨ÎÒëÿ¶Žè|D— GdòÀÐŽ·~Zbõ]ÜUEC?î}£D·«Q]J6"N
+#is­IxspØ1ÚÑwrÓñšQFd„	"CTî×ÒŠlœÌ=1rˆ¾\ÛµIÉfgž¡ü“S–ÿØ<ÜÏQÄv[Ý$N	®íÙ?ÝüNŸ:yÉš*Ç¥âKƒw„c‰‚sÒ!
l%ž§‡è_è `0ì!§¶Ø‚¾¥yÈ5xR8u0šîò ×³Á3só=…ñ¹Ë`>ê÷QfÇYáÃŽœ¤·"‡˜#Œ³PÝ­Œ²Ã}þ¶Ðÿ3qmü–£(ˆrQA)aë_	ÊƒÈ¶°‘|ÝÝGâ@u`JU~÷Nûq5>î,V†Mb¡6Ï!»mýu2WúÐÙåÔsK½e@LRpå]ä£ä'O[©ÿ ÁóœŽ¾ÅçyÍ0÷x3f§ÜÓSØ„B`[:bäžê·¤sú¬Ü$•´(²¨™{ƒËÌQØQs¥m

ÑÍJDïx_ø^×ÄM w,ÛO
V»<‚/OçHUèÎ©5Í¤ô¬Àëv˜ƒÿÆÌ]P{ýþ6R£Î›¼Qj:­õ|»ÛlBI�½°æ3Õ€¾§4'ØS´œ9Õ¶¾Ü$»¾&×„Š=¬¥ðF\1ÿ$_ê`×›‡J>R¨™¦ÁX´•ÎUý˜ˆ!žˆ¥T¸9ƒ£p)Cw=Õ/ŽµG)3¨í…ý«nY^¿V:·3UXÅ’ýÁòÝùÖ6-)[­†#j+pHûÿ“�íFEXœ—Èy>êÜ]<Ìë·¢sm·0&ÎT\ÜØ˜ÓŒÿX‰\öC	IÊWŽ¼Ei‘8ùCï(Ì +žÑßà/Agz$Ÿòê,°?GRÄ®-×IØ+ÊB62Á‰ƒAc0Z^34>ÿ°UÒ¢¼kçbsÀZŠÏ5È¾¤@ô©™¯èMR ždNDp¯ÚÊ:¼q	¸[Î)GÔUž8—ÚCÄZæOÇÀc¬‚p:"TÊ=¦ÝÙù8ÿ¼Å$¸%ÃHÉ“ _|j‹WÝRÐ¦¥‹=×8Íó¾;îiëëJvâkhÂ¶õU'É&Ë—ÐïÃ?ñ¸PnCNôükY÷Ã³8¤00è-ü¼PÎÌcmJ9ÞicgšØ
_åÞ8ÉðÉ8'¼ Ÿ=9¬Ìè£¸Ir¬Ž)Ê¿9ïž<—ñCãXrý$¶s’n¾3v³¢—o¥¾!"…Žš0‘�9.[
¸ç8`¸~zjg©œÛ’#B§›-XRyzðð´±(œñpc!ÀïPé/ÄòÆ¨u˜±UYVËa6LûönÑöæ–AÆ7_³íì$T™Ë�±îM»Ä}‚^Q#§ÑÜì†´ßº#Hàû\É«%À¶­3÷£ºŸúQÎrTPAl&zgÇ($ æpðW½Ï@	½Ë)ã{ÌÖæ Lè.Õ±j¤InŽ‚~Ò‚½/%[ÐÈÇ¾ÑÐ)¼S83ÿ&Î2Î˜©D¯ÓöC¦LÂhñÏ­!	´Å×|PR=tÄÓ;{:1+¯£î§­x]ÝaÚxrq-œ%8ÈÛ°À¥O:åû06ßü,„ø’ºsrZUV–å´>¡^n‚GÑ’ÉUAKãŽ»ëÖ(¶k¹3ˆY¤óêÑ§càc€&U&<ymêéÇñÖ�Âü°z-øW"q¶ýoË¦_µÂ§Wò¡jœõï/o™‹D3®‡íá÷àe@ýEy’…A(>ê`~ý‹gàü¬ŸNÎNÝµHtc¼käØô°ìvÓ‚b‹£ù5
Èoóç
ÓHVÜˆ½Hì­öwˆW«y¬Þÿsœùm5Û·ÕÂŸ@¸uÁw×ÝcÙýÿôž?Û\C†’¿(æŒš‹—<:ç`Ç<ÊìW°¬â³1%Ce%§Ü-‰ŸÛhó„™àÀ�/¸æH/Ã|Œ4ÊêÉš¬Ä–Š'ß‡Ÿ¼¶º§:‰ªØ%Iz²8¾÷Dú°,%wƒëó£"–±í—×m2Ýfƒ)!J”73&Ÿõî"DÖÅÉ…Ô+t¬ÞÃxL®.Í÷CQƒa™86Æ¡ŸK¾êO÷ùá…½š×¥˜5™,‹=TRl3ÞL˜‰õÅÞ´wˆOTâÄâŠH9D =Tø<z„>aÕ�Yà\¿Š-.�P¶ø•n|±Šáªj–ƒ/vn9PpPÀmu€õg™S+m†ë,¢#@,ŽueH¹RÓ]¢‰;ÎjmaÈËt“…¬lÈYVgHŠjŠ2ÂëY»ƒ!,M³½ÊÙ­vM‚»ðšJŽ×â8ÿµÃý£Õå’æþêÂt÷æâð¼Å†â‚áåž|‚.ˆ-b0Pñ2Ÿm?Ï[&¸eämjFO‚¾ßèqçÊ{&±ÇY#ópÕYgÃå"¿6šÙÃúè¦‘ý¾ðIž‘ÞïH[1áž»*°v&Güá5:úß{ÛÜ–m›óÖ�%Sš£�ºc›¤+R"à´º{Ïº$ª¿¡dK2‚  ®–Bò1B0Í—zª˜P0]È}ÿ&ÙÙ½‰1E9Õ0:JŽÃ`“þ¢¤<ÍƒOŸ–#¤AeòÕ[àß«óNœñªÉÜ4>×©[:$móúY¿k¬Ëëç¤AhöI~†5ú¸ÒÑƒOò;:øD
‰EŠ•þáæˆ(Ç¤Ø¼À@ú’Šm®y4Þä\úïÅ±ÿytG0I
ûp[`Wc@å4aþÃl§,ˆò*Ð×ƒnt¥§:‘ý½àoëÞs×ó3Éó½WÕ'Ì–DQáä=‡h@ÓväÕZbNþ1üòâÆò
ŸƒúX´¡B-ŽI’Án*‰ôºUŒ9í8AÀ¤Ô<½0XiqŽµÛ÷b,%A]™ôC(‘¾žB`…!GWü1@™¼â’R—–küpfì¶sxÑ¹¤ú^û\
ƒ zÔìRh\‡G:3z|6×¼QÍë\ídáï6-2»]Kœ>N¡ÿbŒºíˆŠAZpxˆÌ®·G” qo|˜,³Ó0Ç:éI2š”foˆ›ø’y“§6ÿç%‘>å”/¦Ô=0x~ìA-àµ·I9J x¾§x&§6º#Üy÷�ËÛ Ú|ó÷ŽtÈt§²­)_³ˆl$†Èçõ>y>D7‡,ƒ0þÁÛ§³Y1Ú(\q*˜¯”Ù6é¥v@|VTT’õ#…^øŠAVÉbyñkÏY¾¨…£+š@íÃ¶K)�?r_\i}Ñ‹™
Hù~fh_õÐõ)Üì¬Ô²ÚÈG½ZÉU¤™Lhå×Ø.wè^šc;üÜÁŸ´ìG ÔPU
7`{ýoñ)ˆnÏIlÍøðÂæÁÒåoôF–ÁæàƒwûÞÓÕ÷‚a`ñ©ò¨€F˜öL#Ð]¨J§~¼ïHâsçr
RJw]}œ/Ì/ý²æù?= A¡Š”Ð ƒB'©šƒ7zöù7÷¼è%FŸÓ?4BÖá]xÿ
»ŠÉ2ž‡dC\²€¶:ÎñÛ¼#)pS½5†Ñ5[ê·BÂf¤½ÿa·¦enìð™yý[´äëû,£%órIyoBV*šËÁ>×"¥  ®$Ÿ½‘àï/ ’Š—o”áÈÄDäZXjc#q2mCÚ7I¦„iB==¹0Öe8àÂwç0µ_C®‰àRµìK¿Âi@”Hµ|_Ð|Æ5öLÎz’TwèÂI%;$0Vÿ½]dZ)Ï#ZoYngl.ÚE¥Üîü8BCcÔÛ¸Ì!eÞ‘ù2á¹£
c©ô¦Q6‡T[n¯Ã”Ž8†-_Öèô®ïS¿±¶(¤Ê½¹¬Î‚j˜9åûübjªÝs$M<^×PDè%V˜µ¬òiQœ¾9‡\AH°¶-še¼E×[‘UÃúÄ€²¿éå[ÕquÌjfó¨õ�
CD7 Sú3—Ã“¨ãÇŒö`±oéïw Fk„Ô5N¼2r­JÒŠÚnjû<+€V}À%äJE-£ ìùÉLfŽ,Ù"§§èŒdDÖÛŠ‘ðçyžhåàÏ‚¬ßñ[(S©dšõÊÎLO/'4”á†±¸¸kGgW¢¶7f;”ÉÔ{t0”Væ»à‹tõQÉ<6O&Çpt’Æ
ûá"J4\%—3æãÈ±Q)VölÅŠ!ðn~2÷qñ‰qjV§[áíFFQ‡õËaÃßÈQÙÚK}Wª¤± Ü´?æIŠá¥yáÍábóÙ:ú›‘UÜæ‘1ùQ¦#_ÿ•“ÚâJúz÷èi)y^>¬rk™VÃA’Êoö�p#Ã¸ÈîÉí;¨ê–‰<
Óð°Áz++ƒùÕÆy|ð4€Û�eÅÎ²„¢3(1—³2˜z‹ÅÚ 86¾HãG,hb-LÐ$BÌì?É–®%j*hóàO@
`#JøBé­nº÷`×{¸$X©[ÒD¿Ä‘¡Øp}ƒÖFo[ªBøoô4fñûPqPx xQŠ SJÿýŽš(íþ›i‡å)UŽÒÞ$yÏ¿Yéð•µ?ïð-œä€¡ÊƒÝü5¢øî…4Ûs­Lé)Êvç¦gÄŒ¦úJH›*½ ®K³Gò™ŸX^²ÛóþH²ïk”´Lm\#=oîö9P6ÙY‡´V¨êéxC¡ûxÜ‹»‘þW5l<]ï#“ç>`Vºu$“º?£‘èšñd¢‘ÚööÈÜñW½õœ¬BÒ3ÈnÀ)ï›ÎlÄuFhÖ˜Î è¿î„¸¿Ó§…sfíT½©|Ö‡±Szf ¬É± ©lŸˆd(žÚ •¯Ò;…žg;õktÚ9£bÈæxCÁÊçþÈ$ä¦™ïk7ønÿ
(v’€A.kFTþÉà•HìGïé¹N\«ãr’âõ}^]šÔr´ëKúNd‰E½UDýßŒõò“ÿÐøÙÈHn!%ã) AËÞÔ£æÈiN”Í½~Jˆö5%'–¿
¤~g7[E,–‡…8<BóEpÿË40²ºÎH´cZiAz”¸nyôè5©ßÛž'ì(—œÒÝÈoÚt×é¡$¼Þl
 RÆZ/N*žŸ0“–„?w…†ÀR7¥²¼!Gà
�Y1„Ë.^÷H^FÆ-N"l.`èå	ªÀ4d:úiÑH=5¿S²èNš\…“mÅÅ3¦“-�ú‡q™ëåG­Ì\=‰$ÞbTˆñë†ÃWüM3zûdÒuùPþEê^Jð(5R	l±0 Œ¿ïSjÒIrëj”Ç¶öõ˜Ç#Gön¨ìÕ½„ÎzQÿVø&D?“wKô‡
è¢²)ñCó®R£.(*þÂFÆö–eR¿Àc #¯‘,QH€õ˜j bUA®ö Ä‰ËØuHÖ¸}”ðkçpþ…p"äPnÉ®–÷ÛOæÄC† 9ÊÉnH™Þ¶ƒfÕáÊýGèä›¤J/¼bZŠ/Å`ýJ^ÙDOsòØ@åè•P÷j[ýšë”(|þaMÙAsüÎ¸MÑw«_AtQ´ÑI^ÀgvºçL¦º6Hî…dCÿ(ãÙäðìLC °°-«t²oXØ%É‰÷­Ä4¬I»ýç]QÐc6Ÿ$5Ú!&ozU»–¶1D°s¥h“fcøêÏ=Ž0´
.¨Myõ9.Ã‡\mÛ•ºÏ´ÁÙïË‡õq‡zì†Q-‹)ƒv`Æ4³Hé¤Ü,4óÛJ®”$Ã[Òa3â:F&b	ÓN	°ÝžxVÏtö^í{‰­ú%È.Ò³b¹Š=OìAVÍV€ñiû†JÛCÕ·åëÄk* fö6_zÔ¿=T¸,~c1‰RPÁ
Às]ôÔ]¬r EI”E’ÉúÇR|õ>Ïf[&2¶ø'å1ÝÞü–	·ÐŽ¥HI8º®
;ê:+õhÆùšŽäThÎ²\8©Õ‚E.n3s/øJÊäXõËô-mõ±Ñ³
˜)ý¬[j×Ñd6’‹³×HÆ4hkD1(905	ÐÁXQ¢[O}…ÃÜúÄýyòg7I1íd”•£é0Eka£„ÉZ–³àß;T‚¾i+s‰O›ùët~X¥fA›¯o1Ø¢kXdçˆ÷±Ìr|2"y$9èª�Zœ[äÒ>š© Š€þ(Í„`jj ùxïGžð²*ë…ý½ÐÝ¼ü
˜ëÂ¥
ËjG‚µ¤Ðg…=7j#i=wKš7UKì(åuªžåÉ«K5“F=”´¦9Xàòïý¨VT\ÜîÕŽðæ¥üþ@ÒBµáêX®À†*záŽÇiÌGðxqÖôÇ¸ƒÌÂ7Çå	=Ç.ÒDàï©[H›‰SÈ»¯0231åW›êãx·y±V8´Ì¶!üRÉ¬6GÜ/¸óù5á€›§
âN&O«Ø'Ét=›[¯j{"]køà×Ööþ0·@²RNEµ²…ŽŒ<¶7©ÐŽe‚²;Ð#eC²…‚�u]UxösÓ€Sá¦áËºþ!~lH—X„ŠÐ,nƒ' ü…¡ f‚tçŽN�ŸëßIA'ÖÉ™(êLSÍÂ\?ÉT	ØÎWÈ¹»yV²ò(Ao~öŒ`Û¼ùhôÐQm69ØCm‰ÿpa—èr˜@M

kÝ;ä:¥”°{ÚâxcWbvÊp~ËMˆ”¯õä	ðG3t¬ÐWPJF¿~˜[Úr`„qÂŒôA¯ä¿³QÃÐÛj"™¼^¾œ±4Ñ³é±Š#ð[/l£ðFø‘‡ý~ÕoÅZ¶Ô|«2·FMVXBÏ3
–.^—ƒœÔþåxv@Ú¯n€è“k˜ v˜ÿ´æQ_e!±4F¯C[·jã\Û¢Å”JV­ö»H®ÙQ,å™U&ª,²“í-êÃ¯¿²á:@L
OÙZ*È¨ogï<ÝÀ]=ÔŒaZ7¹ˆ›Ñ¸»¼l‹\²C'žë€MaHhLd‘S–ÀYôv-Û37­äˆtŠÌ){ÈÉ,htfí½àsõS‡]–J¼J�öß	Ü(5aµuÝFóÿÈO€Š/|f)¦lmïhžÐî}¦ÿ½;õÄ©k7ƒ$É„ØŠ#è~š™ˆ‡óÕU óÚs*3æóJA^`V;9Û†Mö7%ï”>Ê2	¢Öj”`OC<Z]ì}âUÛåj?²ÃAÁÂîžDÊ¼Œ·OqŒÉ‹â&_¬´ÝÅ"… 
÷ka‡·³p¸KÅ²¡¾³d,Ÿ�³Ëkkø<ý;@ª®©v)âPkž¹ÙÝüé°:£…u:¸-¯†Wp¨Víû‡ÈËígFÀrùó$þ}
“G/#O·/ÿ ÃB5Ntž(Mâtàc»ÑðÈR+»ï\°’’ýµKÿ*Ná4LBèÝ±¶
x ØuQ{1fœŠ¨²–-ÑÉ……Í,]8â?}Å3Î|H}ú,;y=cŠ·óî†©Wˆim¯ÅBÊ„‚‰©˜>£\#Ô/&Rëd5Û-ŠÖÑz„þ¨Z`e¥üsÔÛÊöÙØøÅhWå,Ž¯ãT>uŒàçl¤íj±¬¨]Y«>HGæ5ƒ,È3§}ŠÒ+¹UèäŠI¸Âš âÙ Z|»ÃÀTÏÉaÝ–ö‘õfÐp¦bö [ó¢Úùˆ;W‰ kí×-²ýÛUøí›WÑÌ!"Ï‘Ý–\’ÿ\¡Û{VCåœ²‡sñª
üÓ^àÜ-·uëÊLQnæ[»D¢�Â1ÀØm^õiÅrjìk)æÓdFžú‹}SÅÇæ‚Ïf²Ÿöª­Z;~¥ËÌÏŠòËÅšd@m§‰-sñ]­]ÜZc vjØ‚pœ®hr¹}{þì{-Aî!Ï„ƒß\®é÷v|qKN
IBe4]
Û‘9@·š¹­·Uo°pô½ÃŠ4>zsˆK†fÜG³
4§ÕJÇ½{†Mkáï¸îMÓRóŽ÷<Þ‘•
q_f2pj¼´“8L¶X»BÔ‹ÛK¦à¯ÏGÃ0Z£ª7šÜ(T&à+Ü)Û‹§ü²ž&X®D*H EÊ^Qþ#}U9¹û^¥A	¼.5œ¤›!'1díxÅç.²PÖA«VÜ´¢êJ‚D8^NBe¦¬ÅÊÊëÅjZ'Þ/'Âª€'6þ�‰Š8½nMjÏ…™ª8ìPwåÓbYi¶Zt'i%ÐyÚÂ�Ü}"ñÿgÈÀh
<¡è{9eT’¬\TI‰ÕÁš«ÿLìþ>¼¡öºL	×.ß·ò’8fÞ5cèšeÁ‘ÙÌ®9ÿq|§ŸÜ+ï1›ZvO³¹ºÇkf¯V0Ðöä¿iÃ|Ï0$™8IÞú0Ix_”¼ˆºAC‘ƒƒJFÔ¶üxþ%¨PÏ\¡äF›®Ñ¾P÷AL^0ÿ‹”]è?£Ê½Ot<ÿ›_úfFó/¡Ú»©ú³ÝéÄ,ÎçÈÎ¨¹&]«ïœx :€¤ðÖáRâ#›-ƒzŸæ·“òPšuo]Ë²ò/Þæòàû†ïa]_ý×0Õ rpÆêT#hj—9‡s‹ûGvˆ>Ab²Pwz‘l«÷C€ý˜…ÐÉ#7X‚�
X”šfë_AÃ/ã"‡—1Õ­ÿDJ³2™õ¶}ç¥ŠItm¼UÎJ2a·K°0ŠÀ`Ã—š üøüëñÐ1÷HÏ¢¡ì§%ö²@N=’!€Á@¯éÐªíÇZÌÊæ”OÖ0Éµ{p•
¡›ÌtyÁÿxy‹B(\`×Oe5¬ÙÝUgLAÍ!½ÇwYSð!€+SÍ²Jì@‚2àkw•\Ì.Æh:IØù+Ïzpö1(òÒ`ž€K‡ó	Óö™=ù:Dâb|›èžˆ›,ïOÛÄPÑˆz½J8Ðõ–w…�JÃVøgƒ
Æ:
Þ›ž/ ÅŒ†)ì×¶IQ5j@IÙ$=å<
­+Ú£`0íþ nÕ†žú,>ÇF‹EÌ,KÉ[F)ç•à¿w
b=T´,¢1X§†Ç•î/>¼‚þ
íç-}Ì½’ôlòozÑàÓZë9Ñéìk,‰#ô³Å{^e“¿œ.ÁûÌ²Og¦L…:1%¹›”7;:ñÛ¡`]ƒOX_ô&‹xBuN–q¤0¾³ƒšš5»Î~˜ÚÊ!7—H2dÆ%çvF
¬¦*â7žiM8Mbêóh-rLè**ú£æÇ›ÚcaC©­¸ãýv|qG²ZW2Ý}$ó–Qƒ®Ï
Fãú®8j™EJ¶XúØ˜Þ{í:ˆ¨ä4pV`$ÐÓ…svˆÀ+²™¥8LÜ„mé#ÝRHiŒmÁexv ÿôF„×.ÆF¢óûŒðDA.Ä)óˆ·›/¥œ_À8\ç•!e­J3]àƒ¦Oû|´/Ø'Z±mF6ïÄUteÞ]Åé‡ó\ßüÞÎ¿»»YfËñl„`í¿â¦@&«B~‹|G«æZâöÁƒÀú7å]çÄã±~_ÒÍ‡]üðÕ
2@sÕ2Áá@ížÖlï.¿yq¾v¥SDcP£ÞîZlîÁ’6jÎ‰9Uj{ÀK3É;Î¬T€´-e&ÈlxJ°×�š«:ñ¬Eû.rÌí_È&Ë 91PòHÍ%¼TxslÎjô7N‘æOïåý Ð´Á·€™ ÌÉ'
¶Ü¨g>‹µpaÇêS·ð½2	~…5>=,‹Õ_°Lœª*ÆWc¬—p9Ü€Åî–ñmÖÏ@²k,7�§K»IÒØ:¸XßK×ßfµåkf…Ü»Ãæ‡=¥cÿ\äÀìƒâN”µZN
Â?;¸?ßd+5åJÿíonFÝ¯ˆÄà…T]ôg<D£Š‚KäL¾`$¹K½S¾%›%áä¨$‚t×ªä	Ñ¿ˆ8zRÅFu4q´Ÿ’ªm½¼Agù8–!ï4!èa/~ì˜8Â¦œ«ììFÙÕßá±”êÐò»fŸ)ª¶t]+ás )‡]ºŠb(y{8--’ÞÂ™¯+QûízËÁyÞ÷´®’B…CÁîÚsIÐÓÏú§¢SšéþÙðEèc…éÆë&7;p¯ñHc×à¸¾pm¿5®vkÂ!÷¥/ÜñÌžpeÆi¢ãkxQÈ¯0�û’HHŠ¦f"Öàj’þS	ÝƒîKl¨ñ¹€ÝÓ$É±å ‹wO„Zš6lúÛ’S|ü­Y=ú®Ú¹À.i>Á °åh›!HbÔÑwô¢ñŠàGC®ÜDm#ð35qÚèFÐ&mPåáR|ÄƒVà$ØJÕ^ë½h¿h)žäðäõqäå6rò	u½¬êO­µbî5Šìºðt‹H¨‚X\c×YC:KJyÍ?Ž7v¿‰sö5(yïÍ„œ²3Ó.É§–«¨�BT}·‚GÄÞ3ŒÏ¡®?³ÛÖŸÌ… 54,ùÊ.T¯š¤-Ì=ŽS’Xî;Ž]GåÇMÜp	5<¦Ž˜6!VÆÐ–f•Læ°•ÖñjYA˜À…f°uòøàÉûDq÷D.žW	äzg£	(c(Zà¹Ôª·]Ò“½Ñ†Ç=OöÊ³ÊnnÒøÉ÷óÞ{ƒ°ó	aÀP&A§4(K-\æuvk°e·ª"—¡%±vH-¦Þk0ù0¾¯	Ûó~Ö�€¼œ%!-‘—·»çÀòÝ¬¼£†Ý)Õsë›­W´¶cÿ/Ýc9‘ÃAÏz&?Ð~W,àÓJÙÙD?dÁÖq–üÀ¬&Ç¯\ó RÈ8’öÆ÷I™˜oãèZj‰,)+e‹ÓÌS|]@ü2ÛÓÖQ›¦co„ –FU`4øÉM³1|žlÏ›_Ñúø•„dPîãPºéöÂHÂÈçMÓVŽ¯§Ë°b"•7IxÄ¬°§Ðq8 ×¾vkó3öEM|Ø€ÎŽ‚™kÊ4š¥²ÁÚ1ûžùT¤ý
qÈ\¼Ø	µ«8~‡¥“ÆØþ@Šw Pv1º:äåd
µöS÷¸‡JŽÕŽ:¯ØŽÄ™°Íãd)ðâ,z lÚX5?Å“HHÇ±øPµnÝxGÖç–Øœ²\Í…àÑ³{Ö›áakÅ£N­‚AõÝ¯ºPœO"xéÍ®?`T+csä\~±’Œ…»Ò¾…gÉlºiÄ‡ªÛ‹1")TÔn%ÝE«_VU)¼jµ%`K6/:ð¨³Ë"	
N¿Ÿj;Âý‡óÝ5§ÒÜšìîrÕ…­£¿vØ\|:1Ó
ÂæîÕ¿2	JE�Ûø—rÔifóë+âølÅ
³•ñëk
J”F£Öt»zf'cC­6”|êè.ÀFÉâLòîglÇÐÇÊ@]oWkäP,•Ï™\9{·ßGšãfÚ—ç³¡Å'Ê•o=4 ƒvÝ?ù_GÏp«ëüo,xk¼,bävc*éÊ�]že9M(¨°U+|5µÒ<µ#
bPø®ˆK¥ã'-K\›ØÒ¶Àö.ÖœCU±…rg~Ä0s\€òÍ-$J¶[-üÛês5*=²JR†A2Ùaù=e=»Íù­k¶æ-ýs®¦Õ	÷€ÆìÞ%ójø©D„˜‚¤0¿ËálÎÆ[kWtÐ&EýÇß˜q¾UÌYâéÁªohc‰Ù©£*ÜÁ2FSˆøÛe:ý²ëKA¡ù^(6Hp„ðAnunŠ€_h4[l‰‡e/`ÅêÌ
9-éi62gvº„ñ†…këIå[Ø	EO�6Õ…òÔ$ºÙ•ÂsQ‚ÞÏ!×Ëv>Ö P¦�]õ]5#ëFÂB¶:Õ²QÆV¦í¶ehÊçØ^|‹*{JO¯xz¾/Î§ŒÓE+‹´õEHƒlÌ0Ø1‹k`ô}hX;ŸÂš†	¹ëâO®·ï08W¶0Ø˜ì¢z¼”êÿ2Â‡éš$g@ß¡ÚKÃ~)¤Æ§à1½¤}ÒÃ^´3<ùVÎ[±(—x^ãTp¶ºÚ“,i‚ÎØ~´q°«“°3Tì™
ÑÖâ¦³Û¢Ì>-f¸zÄÕZt™°nO]âØ×+Î?µ…•Ÿ¡Ê+E\,z?ÄÄåq’'9µpq—ÿ|ë¤+t²&ÿ)‹¯_f»s4ãxû,»Æ„^²÷aønQ‘Žmüdá\¬®àfÉ1²&FNéýãÎ<§uönn*rºXG•ëÅæì¯›#¨6#´vb Bèñt×¾©¾Ø2×Hü–úqKÔõë
ð„=$ÏuÉO„ÇK‡!'Ê0‡XÞØ»œI}bV—b®	”‰*àù£¼&hÉA­oWÅz¥Ð“ò³rãÎ%¨²åñ‰ßO$ÅŽQ¾aaT {Î)î
€@ýC ^²¨ësÌì#Uº>ƒV6„šbnÂÑ
ŸšWÏz§ßö2Ðg
ùì,‘C¥ Ø*¸p×³«©oð«áÐ©Ò×M”™¥ÂwycF^=¼óÞmp_i¨ûgSî¯åÙ¡4~H³K#(‡¦EfþîÇXLlÑ“Ùå¾!²“½lÜóÀE8ó›¨z)Ï¢¤Ucö:§¶@Z(a1E¥j÷~ÔMkç>Ôô'ŠØÌ`ãnxg²ÛO3w:5ø…iì÷µ°BùMÐÂ¨{„ŽÖ._²Éª3˜wÿµ¬?¬/,Lsg„HïÆæl-Ç¬®Ÿ†Þ’ b£ª?æ0qRF‹b¿4Ú9$ ¬§G¾èqíœX#‰T¢ÊÒÕ‰Š´Vä­Yuº‹á~H$E,¿~aØI›¡‡pž?‰QˆÔAõON´("¿Þ×=PE\ž(6%Á¦“@†zynõ0}‰RÆ=
+Q»y\n¶6À-j¡¸}ù£ÁÓF˜ì7”~Æ)6²¹-"	À4ÊqmÑ…0@ƒÏ‡ÖºsÌÕËP†k}B¹5ñ;¦D‚ÆÙUÃÏõëTé5ÚR»m£;_C­„N‘ßBÀ±ˆ#”3N4³Ýf²N^=OÿÌbáXáñ>¿±_¶úsK«´åôð½$h«=±A×’ªá°º;€)¼ðãT¥EÄZ@ö$ÚæƒZTêCÛW¬Ï]PCrâÑ›éÇv¢RJ<«Sâ´<>~§D·ÕZpAy¤¶ÏÁzr¿=N ²HÇiIe[QÕø3O¨PžåZkÀ%} ªñ*ª±ŒgRž£â<\á0ãû)¹ªÞz·ÝÈ<Yûäµó„Ù½Þ@î
Þ
“ýY\ŽæâSÀÈ;ñÄ¯Ásì$ã|ýýñüXç
ªàÎRFVJ¡p¢œNO
ÔSrþÐ€‚„öe}Q<†¶ç|›”0¦ÑÈÄQçBkæ\ë:x\G�çS,š"u çPEÃéC)Ù
½Yâ[œÚ¥»©â• =)$/™ZQù¨¼›KTþH3Èz©][<èo�‡D…f«!âWèmÁˆ@Y$ÊVa·½-¡D…ÆS2Æô9ÿ09<Ãm&J52¹B-¢ðàžq%˜2ãûå‡þÐÅv³Kä\•lñž¨¥ÃÖÙÄ*’t#ê!ù£ÚÈâ'ç‹A^ç-ükß‡w".M!0‘\ó/æ/àlž€ö¦Aþl®ÑðxµQ”PFÛÃzÆ§ô8„Ê‡óâÙã…•¯ªò»ð;êðµ†²>’™
š‹†`WÂ™æ’QøÛ
×³¦œygWÌµÐ-ÓS}?BK^ŒƒõCéµ9+]‘©l1º[”ÚAB’: Èe–L#Bk:X±%5�À¨å»d¸kìuƒ¶²{ˆÑ‰/ÊfŸþhè[çö‘õÚ}2dƒ¶l£gÀn*2„õŸeð§_¿º>.4uÏãÜªÅ#ÜpÅl«}îÈÆ…xCç§¬».”©eySÀ@tÊ“<÷èTü÷ÂƒžhþéäÖ†á¦‚zÔË�=F®åzÙ5$Ì7œ°ð’Ÿ¹šY&Ã¿êû0ˆŒ×eèQùà´šm
˜ÅÈ×Z
=:ËJÓb¤º$üU$hàöJb•‚wåm’øýëg´&#¤¬‡°sý‰=gY¡:àP¦Õ‘:UŠuÄíâå1a3J†]—*
`ÆŽ-K7´ë;	æÁ×€>8õÔp)¾¶³"6!HiæA¡ÒCÜµÝ,pN
Ø8
ÄŽ…—¼P§xñ€Þ÷ª,�<.'Eží¹÷ŒÎ,Fs%ì%©æ}‡³Ôa¢ÖîmVw¤±20•@{[K<nð@ùfã†Ê¤T¿þ¬{ÕÊÓŠ­€7—õ¨K¦‘‰c¢ŠUhÚàëcö2¶ÎPGþòsj±ÑTÌý‘¢Tšã6>ÝNzk‰+Âü®#Ê*¥éˆu°/HXš¡ôðSÿ¹ûœÃÎÕ™W¹ê0ûÛ×á7¦‹ï¥ºÃ¿u–ÿ¬(LœÛQÀ×®ûˆŒh¾Å:uG¡Ÿ©hÂÃ®ìøðÇFÊˆPüA­OÂZdèJŽMÔ%Ë\Ì.·Oªx-11 ¥6Ñ³Ë^§E¤Ž®´zÓ"°ïšZ™æã=KZÔW±Óñ…E`«‚»E”Érð-Ö‹
ùéK¼½ÐðìOÈÊíªA{RÄÂš„P±œíªB[käLºˆ+›º@“Fá…M
ŸDf|\3¨,r·_[r‡!®hïó7(=Ä«´îÀ±½«°‰ð›bº+‘úgaíõ!ïä2¥x^xv”á}§§Ë‚-ÑÃjãRVëÝ¹V4VÛj!¾|`ì¦Yj¨u8i Lt¤”,'ö?$ÐŽ–ïE	f€Ä¸•½Sû^ˆaÏÝ’?ÓqW#í¨AÒòñœ„™d°¢WåÈ=Z,ï�…0•y<XÞWáˆÆaÄWNrUô‘sóô #¤hS¸æè\ŠÙ=t:bòúÐi®Ùù²°•VÑfôq³V"-æ÷„r]¦xýíâÍ¨nË6Gêk’ï\áŽ±¿ºP8Äûˆ´3¿4=mþ¦ŽœFÌ}ˆ°?¹jÚ5ôÎ­IHŒ¾3Úþ¼‹_ZQrcâ©pîŸUº§f/®c§‚ê0©‡“ŒZŒMºƒåKî¶rŽ0Æn…1�ÊyKðÔ–
OgöfiBPÍí=Vg‰nOQùx2õBÞ;£5Žá˜æò@…
©M¢Œ\­»ŒA‚Fº·šø/ Û¯-u•þº*K‰âÖçJÏ‹/þÌÂ½¹·K™Ýéãóé[®†ÃÖÁÿ÷ÿVç0ái`p«»éî E?¸ÿàRÅ
ã48þÑ>[B¶¢r™Irƒ<^GRƒP)jL*iñ¥«QLÒ²—ÃˆfÝ_ºrÑ»óÏ"Þ<êðG`|®-ŽSqØ£ÿoÓŒ¦çUði£r4`œ‡Œ×Š§ïÙ­DÔ
¹Eõ+Í;Hþ³Ž"$Õ¼1•x
j«rþ¦Õå‚Sß…6ÑWÂ³«C÷#|»uËÞ·')ÆÐh’oÃ´@2¢/çsö¥î¿A(à î¢$4…dÉDg©s<ðpžõŸtñIt²QI+F=q U�*µT©AÍ®ÁX?0yýe­{Äöêú¸ú~áÀÂ
jH)âø�àEÎgÔ/áV1¤¡
Ïó‘…ÿÝ«r°>Et²¥CÂÖWÚš('Ù3â³„v ­^ˆÆ�åŠ’ÁhÂxJ	vžlÃý,Hs¨Úßlw~fä#:¤@3@K¯ÃêN%f£6	IG²€	!”û/`>Ë]G³ÃèÎ-ŸÄQ�Â(2¸rMï;·×Àèq*rã¡ß)M<yÏ³28»ÖÕ9Q±÷c†õ�¦¼SB Ú|dÒúíj¿áëˆð]YnaŸ¥¤|âwiæï*‘Ì&.ÔJüŠvwÀƒ5G4»Ä`‹ŽÏ8È#>“�e&tñ/¹èðWE.!ë8Içíå›×Öí|ýZ:*u% ´MFRÆ3žd¿À©Î-LÛÕCj/&æUÞåï´†’‰¡Þ|Ã7h@O,ÝæÔîììÖvDÔÍøüÊ¨+(A¾ákg¬ÄãQÌ¬ˆ{%èŒ®’êÆMøQp.†mFbxš„!”E5µýÝ!båÓhÞdHÌ-»¿O¯È5³ÿ~½‘!ÃBpF[#
ªDPqDð$ëR`§l¬C,ÄÀ¨Ð^†¹Ñæ²W×Xf†%ú¶Q‰"Ò þ>]Ò^> õ–¥#W	~Â½•cvWïŠK¡“gÈ†.ÏYbÔ;óŠ¬iŽÏ¢ƒ½
„ðŒù—gìÉTÉ-m€~a"ÒTí,œ¯°{¬p³!é'”Ž‹P2ÕÇLdˆý’ä\¢YPëØ~¯êÈå"ùÆ?K³¯ÐqßØ#¿º+m=TAÍ-ö×v

NŒûÛo�±Ý†å´¦<(c8À"iO'<]Q-&ôg’O¤'ëÝD[;_¼]e»ùUífãF„{®ËÔ 8ñÃÁ4â9TM“8¡ÙXì¬ÏÝ‰FòáóC÷D²ÿB|=º§ DL%‹=¹lùëµŒ§¹†‚Ñ$êÕÂnÿ%f˜œQ˜"ÖVl·ÇÎ®ë{ ?ÎÔûŠð7ð‹Zû×®5Íö/™Q&1q&‘¢þD(?­ÙéœW_¨ç®È*¦z…cja_bJ10NgåzL76ea.ð¹¿d>ÆiÂ–ý¢’
l¿zäužaBÍø~½æl5ª'’Å!Nñ5»¤ BÑÖ›ÜG÷R5(¼žITË2mÿä‘|¼X€SÆp=ZÇ3ÒI§†qUeˆ[@Íž€³:[ÛQcáö§Æëé¡Ý‡‘.Ý¤¦±wÝÙöÑ®¿
‘ç^Âß— Ò¶ý¥øÕz’]¬MËmtšãò=Û“@–Ð¨`st6`ÍÊðiÄ,íüço‚éní2zëR5äíädÔ³qOÆ-½žYÄÜ!r®ŠaÎ$,ØÀÜ¨”"M…k®î:…¯Ã©X,ÊˆÇŽõ*PÎè~Õa»vj-áÜÅŽ],N'·Fý–úwî¥@¤£™üV«\Ù$P~nöxÖ>²3í#®Ù×­e!#O²à ¾¿ñ–m›á”Ò!ÿæé5kîÈ`­iìRP»|JdbÎžòÅðÄ?}C<É!OÁ™È¦Å¯[u%ê€y8H‚™ÿ,›FÒ®X‡<—žŸbL‹Ey+A±[ÿ?Y”ÒlýŽLN¥ÿÚùZæSáEhDû3¦¯ ï’÷íd5Tg,L3šNºR¨CEŠŽ0LvI@¦…uDð·Dh¹É1+{ßj¹2­z„ÀSx˜ÊIçˆÔiT®¤7 œ“él——j™&_pß7Ó
)Q¹	INk0‹2›m×ls1¶.MÏ¢ K‚$rJ6x¤üo©ŸU"	»º¾‘>Ö{²2xaIx”K¥1 ¼’«Š\QÓ~DG–‰z,ØÃŸ{‚R¹ŠçhœŒvÕ* @o g§ý"½O}ˆs£‘—€¤±‰94!zÝpUN%9úÓ
nñ'Æ\l:^*a•ÅŽô¼¢”?t™•‘æJ¿@ÎNNƒÎ…’´I¦®½nÿ×E¹,¯QÌ¬lícÀÂ¤3k˜=ª­«SœIQÑ)0;,†¨9Ðæ’Ú'–Hxk`0øÙã¤B¯ìüxÑýrÀ•%É{x7Ÿò}[©„¢ÁóÃý;`ÑaE…¥e—ÎË¬Õ€Ó>Rø@ì
ðØf¿j@íÉ†ÅiQEÕ†_ñ¶[]$\ìâþWg£t©‡Ö{¬|e€•~{wà=93jbQ™ïæÎÏÃâ ·%žóeÊ– TwYUú<TœE
6gvÖÈ™9¡‰C+Trß\ª¨]¦Œg¬(ª¿ÞDC‘åæpÏ©geœùÒ’OŠâúÛ*Ê5ÑÜ;WâïQÎ Q³Ì?¢PN‘Þ÷š§üI]‘|-ªf<:	 akð_“ç~*†ƒ1[hK´q€k;¶©Šê®
 g²/H}4I­—z‹fÕ6•Ÿ)åhöthÛó÷2BÁôóäTz¯7‡ÎIÃ‰® zS¸Í± xOmq5ÍÊ ƒJ°ªÃÄ«Á1ÁA%Ù@-±ž$¸×­×lî«œa/?¥–®�¨ìh%qrÂÄ3kQYò€òœIqÄºj£=‚_Ìð~Bqþ<s	–p<ŠáØ£çÌé=Ož1\Íüƒbcó¯ÁþlUd“/}h‰îÖw:ÜîHä\6yD¥+~Þuq–Æ
g‰{Æ)®ŠStö}tç9CÖ×-¬´òxCg‘Gº¸•l‘¾ååïUCV•š} ›™I€>ì`4ò‡y“.¡œl˜ÚK¯ÛA÷`ð8b$†O‰°&‡9®R¿3™³J�­Ø&Q°!,M9/Ø‰ÆínhN2ÆÉé=/^Óå¹nØ|*D¬Ý"òw;Øºû6ÞÜ9xu-5Ï7Èt\2?ò°³Z)zmÚ658ˆÎiÜQ¥@2Pæ±•k-Ò*¿Ô#ØÒ,
ZŽïøªèTz;"æ…ÔqßO¿i»úþz¹7Ï>Ô2­;ä r;—AÎI¡4çÿòúSK×sm„?`9Â€(Ó¼‚ÝÑ'ZÇÂ¬eA|¿÷Çsç_¦ûáÏMMGP;&%ºöÂ~vÑBw±¾îOžy*bûŸb©ØãkdKrsÞd39;##0`J¾¢ÕîîY²¸Al„KtP~øÆ¼¶5XúlÖ»»UŸÁ£z/å?-"nÅ.Ÿhn bl*›Öàã¥_ý9!ÞÔodHžÕý}ÜÇ!á1Ð~9øª*ó‚0CFÑ™Æ®+¢J=àÐgš’I,ÿOÞ®6>3>”È½‰8¤7ùÑ#ÖŒžošbAW“YÊz†8ÊŒœrùÚ«À¥Š×YáÀKçFšŒ³‚Ñ)öL%ïÏsÒå)wvq «ÛG4ÓF…Ò”o°Þ[_ºÐ %jè<_‚(A7ôs4qÛ9›Þt$7w<¦³LÓá™vNæ2é$[ŸïDNæ„Ý>›w ´Èm®Tt9În’ót3b©Õ|IDe¿]±‚T˜y?»Á`Äû §ê7²¢Ê#ÿ¨Ì’Âú¦¹ŸÙp»eÙÞÁìþW³¬¹îC‰ÝQIê˜d['¼F|—Âq¥ÚßTP&"7ÛÙRL¯Òw™c­õü‘ø¸AzùÊÑ¬yq
¯<—{„;U„XFh”ßeø,è5­“Â%©¨e¥í‚¾)ˆiôÞw:†*BÖ¹Cü•òÆ2xäzžœx‡÷ìýüäVRåXN
6õfÁioá…åQ¯®NßX÷4{¡BÀp!@ºrÔÛhUªgÅÿ}?ÁÔ«í¹	6\•5>hª4¥ôj¥Ã6Î>äO¥z·2‹#ˆ\nÌãÙ/3ØýÍ–}Â$x&^R(†v"ü…ÅŽbfsÆ@Ž´}*—
ó»\N¯¡h•7Ÿ˜|KŠ…»Wô¤¥¤HÑ÷Ù½Ë¨rçäîñAPkÖ×‰ðý·§?yQ.&†è±€å^]Œ´qÀ¢à"¯õþ®ä—ÞË‡Ï+'ÇXÉÉr_ufV¹áÁ5Ûß-?ëë¬Pÿ]¦¶D<_µî+×Ü5Ê;Þ”bËòÒ‡#!L•!cÀ=*%àiTeÞ%oöE%
¢‰„4¥¶Z1@,º$‘5céw¢¯jWL÷(Gä³§<ëSÙN Í’ÔÝ%’?ê_¼Ä²×£í¬5^d~Ôˆ´OÔ?§„^úíWÅA‘›Ü)ù
endstream
endobj
46 0 obj
<</Filter/FlateDecode/Length 48>>stream
lD];ºè}	Õa„Vî²ãOõéFÄ@‚‡Iœ$jÙ–ù£!’OWiœ9fªkF—êšÝ
endstream
endobj
51 0 obj
<</CF<</StdCF<</AuthEvent/DocOpen/CFM/AESV2/Length 16>>>>/EncryptMetadata false/Filter/Standard/Length 128/O (He×NR\016}^àd6.ãd´0W¯×E+”M„1Ò5À‹\025áÞ)/P -1852/R 4/StmF/StdCF/StrF/StdCF/U (’k…\0026ûoþŠ:•\013\007u¸k\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000)/V 4>>
endobj
xref
0 52
0000000000 65535 f 
0000008025 00000 n 
0000008529 00000 n 
0000008070 00000 n 
0000003305 00000 n 
0000000117 00000 n 
0000008586 00000 n 
0000008720 00000 n 
0000007732 00000 n 
0000004720 00000 n 
0000000015 00000 n 
0000008849 00000 n 
0000009178 00000 n 
0000009317 00000 n 
0000003496 00000 n 
0000009755 00000 n 
0000003597 00000 n 
0000009960 00000 n 
0000003699 00000 n 
0000010179 00000 n 
0000003801 00000 n 
0000010335 00000 n 
0000003904 00000 n 
0000004006 00000 n 
0000010483 00000 n 
0000004107 00000 n 
0000010956 00000 n 
0000004210 00000 n 
0000011441 00000 n 
0000004313 00000 n 
0000011740 00000 n 
0000004414 00000 n 
0000012221 00000 n 
0000004516 00000 n 
0000012764 00000 n 
0000004617 00000 n 
0000013353 00000 n 
0000013932 00000 n 
0000013958 00000 n 
0000020224 00000 n 
0000020512 00000 n 
0000053428 00000 n 
0000014396 00000 n 
0000015177 00000 n 
0000053543 00000 n 
0000053822 00000 n 
0000091011 00000 n 
0000015853 00000 n 
0000016642 00000 n 
0000017350 00000 n 
0000018787 00000 n 
0000091126 00000 n 
trailer
<</Encrypt 51 0 R/ID [<1642764f97ab0ed14e171a1d8a93de2a><d123cb2e390b45d696935945957904b1>]/Info 3 0 R/Root 1 0 R/Size 52>>
%41185d51758974497b7c30da9f72c5a686d7da60a2b52e16fe73a800d0d20307-7.1.19 for .NET
startxref
91426
%%EOF
