import boto3
import subprocess
from airflow.models import Variable

TMP_FILE_LOCATION = "/tmp/rust_executable_tab_conversion"

# write a Python function that downloads a file from s3 to the local filesystem
def download_rust_executable_from_uri(bucket, key, temp_dir = TMP_FILE_LOCATION):
    print(f"Downloading s3://{bucket}/{key} from s3 to local filesystem") 
    s3 = boto3.client('s3', 
                      region_name='us-east-1', 
                      aws_access_key_id=Variable.get("aws_access_key_id"),
                        aws_secret_access_key=Variable.get("aws_secret_access_key")
    )
    
    s3.download_file(bucket, key, temp_dir)
    print(f"Downloaded s3://{bucket}/{key} from s3 to local filesystem")


def execute_rust_binary(uri, temp_dir = TMP_FILE_LOCATION):
    print(f"Executing binary at {temp_dir}")
    try:
        subprocess.check_call(f"{temp_dir} {uri}", shell=True)
    except subprocess.CalledProcessError as e:
        print("Error executing binary")
        print(e)
        exit(1)
    print(f"Executed binary at {temp_dir} against {uri}")