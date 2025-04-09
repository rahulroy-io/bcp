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


