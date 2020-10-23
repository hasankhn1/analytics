import boto3
from decouple import  config
from botocore.exceptions import NoCredentialsError

ACCESS_KEY = config('ACCESS_KEY')
SECRET_KEY = config('SECRET_KEY')
# Let's use Amazon S3

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False


uploaded = upload_to_aws('orders_uae.csv', 'sss-sfcc-ocapi-output', 'uae_orders.csv')
uploaded = upload_to_aws('orders_ksa.csv', 'sss-sfcc-ocapi-output', 'ksa_orders.csv')
uploaded = upload_to_aws('orders_kuwait.csv', 'sss-sfcc-ocapi-output', 'kuwait_orders.csv')