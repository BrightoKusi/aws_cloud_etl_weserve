import boto3

def create_bucket(access_key, secret_key, bucket_name, region):
    '''
Description:
        This function creates an Amazon S3 bucket in the specified AWS region using the provided access key, secret key, bucket name, and region information.

Parameters:
        - access_key (str): The AWS access key ID used for authentication.
        - secret_key (str): The AWS secret access key used for authentication.
        - bucket_name (str): The name for the S3 bucket to be created.
        - region (str): The AWS region where the bucket will be created.

Returns:
        - None
    '''
    # Initialize Boto3 S3 client
    client = boto3.client(
        's3',
        aws_access_key_id= access_key,
        aws_secret_access_key= secret_key
    )
    # Create bucket in specified region
    response = client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': region,
        },
    )




def load_data_to_s3(access_key, secret_key, file_path, file_name, bucket_name):
    '''
Description:
        This function uploads a local file to an Amazon S3 bucket using the provided AWS access key, secret key, local file path, file name, and target bucket name.

Parameters:
    - access_key (str): The AWS access key ID used for authentication.
    - secret_key (str): The AWS secret access key used for authentication.
    - file_path (str): The local path of the file to be uploaded to S3.
    - file_name (str): The name of the file in the S3 bucket.
    - bucket_name (str): The name of the target S3 bucket where the file will be uploaded.

Returns:
    - None
    '''
    # Initialize Boto3 S3 client
    s3 = boto3.client('s3',
                  aws_access_key_id= access_key,
                  aws_secret_access_key= secret_key)
    # Upload local CSV file to S3 bucket
    s3.upload_file(file_path, bucket_name, file_name)
