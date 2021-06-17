import json, io, time
import os, sys
from zipfile import ZipFile
import boto3
from botocore.exceptions import ClientError

# setup variables from environment variables
aws_key = os.environ['AWS_KEY']
aws_secret = os.environ['AWS_SECRET_KEY']
s3_bucket = os.environ['S3_BUCKET']
s3_key = os.environ['S3_KEY']
m2m_user = os.environ['M2M_USER']
m2m_pass = os.environ['M2M_PASS']

def upload_file(file_name, bucket, object_name=None):
    """
    Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def unzipper(path):
    for file in os.listdir(path):
        if file.endswith('.zip'):
            print("found zip:", zip)
            with ZipFile(path+file, 'r') as z:
                print('extracting {}...'.format(z))
                z.extractall(path)

def uploader(path):
    for file in os.listdir(path):
        if file.endswith('.jpeg'):
            # upload jpeg to s3
            print('uploading {}'.format(file))
            with open(path+file, 'rb') as upload:
                print('print out upload', upload)
                upload_file(path+file, s3_bucket, object_name=s3_key+file)

if __name__ == '__main__':
    print("starting...")
    data_path = '../data/'

    print("running unzipper method")
    unzipper(data_path)
    print("running uploader method")
    uploader(data_path)
