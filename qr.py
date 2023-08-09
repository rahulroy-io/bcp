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
