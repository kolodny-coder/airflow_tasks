import csv
import boto3
import os

# ...

# Path to the local CSV file
LOCAL_CSV_PATH = './edge_devices.csv'
print(f'this is the local path {LOCAL_CSV_PATH}')

# S3 bucket and object key for the CSV file
S3_BUCKET = 'dank-airflow-demo'
S3_KEY = 'config/edge_devices.csv'

def read_edge_devices_from_csv(file_path):
    edge_devices = []
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            edge_devices.append(row)
    return edge_devices

def download_file_from_s3(bucket, key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, local_path)

# Check if the script is running in a cloud environment
if 'CLOUD_ENVIRONMENT' in os.environ:
    # Download the CSV file from S3 to a local path
    download_file_from_s3(S3_BUCKET, S3_KEY, LOCAL_CSV_PATH)

# Read the edge devices from the local CSV file
EDGE_DEVICES = read_edge_devices_from_csv(LOCAL_CSV_PATH)

# ...
