import boto3
import logging
import os, sys
import requests
import json, zipfile, io, time
from botocore.exceptions import ClientError


# setup variables from environment variables
aws_key = os.environ['AWS_KEY']
aws_secret = os.environ['AWS_SECRET_KEY']
s3_bucket = os.environ['S3_BUCKET']
s3_key = os.environ['S3_KEY']
m2m_user = os.environ['M2M_USER']
m2m_pass = os.environ['M2M_PASS']

rerun_list = []

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

def runner(r):
    file_name = r.headers['Content-Disposition'].rsplit('=')[1].strip('""')
    print(file_name)

    with open('../data/{}'.format(file_name), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # if zip then unzip
    if file_name.endswith(".ZIP"):
        print('found zip file')
        ZipFile.extractall('../data/{}'.format(file_name))
    # upload to s3
    print("uploading file: ../data/{}".format(file_name))
    with open('../data/{}'.format(file_name), 'rb') as upload:
        print("uploading file:")
        upload_file('../data/{}'.format(file_name), s3_bucket, object_name=s3_key + file_name)

    # after file is uploaded to s3, delete it locally
    os.remove("../data/{}".format(file_name))


if __name__ == "__main__":
    with open('../data/data.json') as f:
        data = json.load(f)
        count = 0
        for obj in data['products']:
            count += 1
            print('{}---{}'.format(count, obj['url']))
            response = requests.get(obj['url'], stream=True)
            if response.ok:
                print('response ok')
                runner(response)
            else:
                print('response not good')
                rerun_list.append(obj['url'])

        # check if any urls have been added to rerun list;
        # if so, try response again with runner func
        if len(rerun_list) > 0:
            print("rerun_list has {} urls. preparing to run...".format(len(rerun_list)))
            time.sleep(10)
            for u in rerun_list:
                response = requests.get(u, stream=True)
                if response.ok:
                    runner(response)
