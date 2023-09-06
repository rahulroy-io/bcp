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


