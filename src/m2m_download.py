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
import sys, os, glob, sys
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

# main function that downloads, unzips, uploads to s3, and removes local file from staging area
def runner(r):
    file_name = r.headers['Content-Disposition'].rsplit('=')[1].strip('""')
    print(file_name)
    # write file to local file
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
    target = glob.glob(r"../data/*.jp2")
    if len(target):
        for item in target:
            with open(item, 'rb') as upload:
                print("uploading file:")
                upload_file(target, s3_bucket, object_name=s3_key + file_name)

    # after file is uploaded to s3, delete it locally
    os.remove("../data/{}".format(file_name))


if __name__ == '__main__':
    print("\nRunning Script...\n")

    serviceUrl = "https://m2m.cr.usgs.gov/api/api/json/stable/"

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
                print('(line 171) REQUESTED DOWNLOADS COUNT:', requestedDownloadsCount)
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

                    print("LINE 193---initial downloadUrls array count", len(downloadUrls))

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

                    print("LINE 207---downloadUrls array count", len(downloadUrls))
                    print("LINE 208---print out all downloadUrls", downloadUrls)

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
