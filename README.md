# bcp

SELECT CHECKSUM_AGG(BINARY_CHECKSUM(TableHash)) AS TableChecksum
FROM (
  SELECT HASHBYTES('SHA2_256', CONCAT(Column1, Column2, Column3, ...)) AS TableHash
  FROM [YourTableName]
  ORDER BY Column1, Column2, Column3, ...
) AS Subquery;


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


