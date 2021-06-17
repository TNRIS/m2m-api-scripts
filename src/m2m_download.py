# =============================================================================
# modified from USGS/EROS inventory service example found here:
# https://m2m.cr.usgs.gov/api/docs/examples/
# Python - JSON API
#
# June 2021
# script to transfer large amount of NAIP 2020 data to TNRIS s3 bucket
# run it on an AWS EC2 because it could take awhile...
#
# imports======================================================================
import json, io, time
from zipfile import ZipFile
import requests
import sys, os
import time
import boto3
from botocore.exceptions import ClientError

# main=========================================================================

# setup variables from environment variables
aws_key = os.environ['AWS_KEY']
aws_secret = os.environ['AWS_SECRET_KEY']
s3_bucket = os.environ['S3_BUCKET']
s3_key = os.environ['S3_KEY']
m2m_user = os.environ['M2M_USER']
m2m_pass = os.environ['M2M_PASS']

# send http request
def sendRequest(url, data, apiKey = None):
    json_data = json.dumps(data)

    if apiKey == None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}
        response = requests.post(url, json_data, headers = headers)

    try:
      httpStatusCode = response.status_code
      if response == None:
          print("No output from service")
          sys.exit()
      output = json.loads(response.text)
      if output['errorCode'] != None:
          print(output['errorCode'], "- ", output['errorMessage'])
          sys.exit()
      if  httpStatusCode == 404:
          print("404 Not Found")
          sys.exit()
      elif httpStatusCode == 401:
          print("401 Unauthorized")
          sys.exit()
      elif httpStatusCode == 400:
          print("Error Code", httpStatusCode)
          sys.exit()
    except Exception as e:
          response.close()
          print(e)
          sys.exit()
    response.close()

    return output['data']

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

# main functions that downloads, unzips, uploads to s3, and removes local file from staging area
def unzipper(path):
    for file in os.listdir(path):
        if file.endswith('.zip'):
            print("found zip:", zip)
            with ZipFile(path+file, 'r') as z:
                print('extracting {}...'.format(z))
                z.extractall(path)

def uploader(path):
    for file in os.listdir(path):
        if file.endswith('.jp2'):
            # upload jpeg to s3
            print('uploading {}'.format(file))
            with open(path+file, 'rb') as upload:
                print('print out upload', upload)
                upload_file(path+file, s3_bucket, object_name=s3_key+file)
            os.remove(path+file)
        if file.endswith('.tif') or file.endswith('.xml'):
            # we don't want tiff files
            os.remove(path+file)

def runner(r):
    # write file from url to local file
    with open('../data/{}'.format(file_name), 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)

    # run unzipper method
    unzipper(data_path)
    # run uploader method
    uploader(data_path)


if __name__ == '__main__':
    print("\nRunning Script...\n")

    serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"
    data_path = "../data/"

    # login
    payload = {'username': m2m_user, 'password': m2m_pass}
    apiKey = sendRequest(serviceUrl + "login", payload)

    print("API Key: " + apiKey + "\n")

    datasetName = "naip"
    # texas bounds
    spatialFilter =  {
        'filterType': "mbr",
        'lowerLeft': {'latitude': 25, 'longitude': -107},
        'upperRight': {'latitude': 37, 'longitude': -93}
        }
    temporalFilter = {'start': '2019-12-01', 'end': '2021-06-01'}
    payload = {'datasetName': datasetName, 'spatialFilter': spatialFilter, 'temporalFilter': temporalFilter}

    print("Searching datasets...\n")
    datasets = sendRequest(serviceUrl + "dataset-search", payload, apiKey)

    if datasets:
        print("Found datasets:", len(datasets))

    # download datasets
    for dataset in datasets:
        # skip any other datasets that might be found, logging it in case I want to look into it later
        if dataset['datasetAlias'] != datasetName:
            print("Found dataset " + dataset['collectionName'] + " but skipping it.\n")
            continue

        # set more or remove filters here; for testing, setting the maxResults filter may be useful
        acquisitionFilter = {
            "end": "2021-06-01",
            "start": "2020-01-01"
            }

        payload = {
            'datasetName': dataset['datasetAlias'],
            # 'maxResults': 3,
            'startingNumber': 1,
            'sceneFilter': {
                'spatialFilter': spatialFilter,
                'acquisitionFilter': acquisitionFilter
                }
            }

        # Now I need to run a scene search to find data to download
        print("Searching scenes...\n\n")

        scenes = sendRequest(serviceUrl + "scene-search", payload, apiKey)

        # Did we find anything?
        if scenes['recordsReturned'] > 0:
            # Aggregate a list of scene ids
            sceneIds = []
            for result in scenes['results']:
                # Add this scene to the list I would like to download
                sceneIds.append(result['entityId'])

            # Find the download options for these scenes
            # NOTE :: Remember the scene list cannot exceed 50,000 items!
            payload = {'datasetName': dataset['datasetAlias'], 'entityIds': sceneIds}

            downloadOptions = sendRequest(serviceUrl + "download-options", payload, apiKey)

            # Aggregate a list of available products
            downloads = []
            for product in downloadOptions:
                # Make sure the product is available for this scene
                if product['available'] == True:
                    downloads.append({'entityId': product['entityId'], 'productId': product['id']})

            # Did we find products?
            if downloads:
                requestedDownloadsCount = len(downloads)
                print('REQUESTED DOWNLOADS COUNT:', requestedDownloadsCount)
                # set a label for the download request
                label = "Texas NAIP 2020"
                payload = {'downloads': downloads, 'label': label}
                # Call the download to get the direct download urls
                downloadRequest = sendRequest(serviceUrl + "download-request", payload, apiKey)

                # PreparingDownloads has a valid link that can be used but data may not be immediately available
                # Call the download-retrieve method to get download that is available for immediate download
                if downloadRequest['preparingDownloads'] != None and len(downloadRequest['preparingDownloads']) > 0:
                    payload = {'label': label}
                    downloadRetrieve = sendRequest(serviceUrl + "download-retrieve", payload, apiKey)

                    downloadUrls = []
                    sleep_count = 0

                    for download in downloadRetrieve['available']:
                        downloadUrls.append(download['url'])

                    for download in downloadRetrieve['available']:
                        downloadUrls.append(download['url'])

                    print("INITIAL downloadUrls COUNT", len(downloadUrls))

                    # if didn't get all of the requested downloads, call the download-retrieve method again after 30 seconds
                    while len(downloadUrls) < requestedDownloadsCount:
                        preparingDownloads = requestedDownloadsCount - len(downloadUrls)
                        sleep_count += 1
                        print("\n", preparingDownloads, "downloads are not available. waiting for 10 seconds.\n".format(sleep_count))
                        time.sleep(10)
                        print("Trying to retrieve data\n")
                        downloadRetrieve = sendRequest(serviceUrl + "download-retrieve", payload, apiKey)
                        for download in downloadRetrieve['available']:
                            if download['downloadId'] not in downloadUrls:
                                downloadUrls.append(download['url'])

                    print("FINAL downloadUrls COUNT:", len(downloadUrls))

                    count = 0
                    rerun_list = []
                    # start download, unzip, and upload processes
                    # calls the runner function above at the start
                    for url in downloadUrls:
                        count += 1
                        print('{}---{}'.format(count, url))
                        response = requests.get(url, stream=True)
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

                else:
                    # Get all available downloads
                    for download in downloadRequest['availableDownloads']:
                        # TODO :: Implement a downloading routine
                        print("(line 181) DOWNLOAD: " + download['url'])

        else:
            print("Search found no results.\n")

    # Logout so the API Key cannot be used anymore
    endpoint = "logout"
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
        print("Logged Out\n\n")
    else:
        print("Logout Failed\n\n")
