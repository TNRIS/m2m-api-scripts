# =============================================================================
# modified from USGS/EROS inventory service example found here:
# https://m2m.cr.usgs.gov/api/docs/examples/
# Python - JSON API
#
# June 2021
# script to transfer large amount of NAIP 2020 data to TNRIS s3 bucket
# run it on an AWS EC2 because it could take awhile...
# =============================================================================

import json
import requests
import sys, os
import time

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


if __name__ == '__main__':
    print("\nRunning Scripts...\n")

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

            # print("SCENE IDs:", sceneIds)

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
                print('REQUESTED DOWNLOAD COUNT:', requestedDownloadsCount)
                # set a label for the download request
                label = "Texas NAIP 2020"
                payload = {'downloads': downloads, 'label': label}
                # Call the download to get the direct download urls
                requestResults = sendRequest(serviceUrl + "download-request", payload, apiKey)

                # print('requestRestuls[preparingDownloads]:', requestResults['preparingDownloads'])

                # PreparingDownloads has a valid link that can be used but data may not be immediately available
                # Call the download-retrieve method to get download that is available for immediate download
                if requestResults['preparingDownloads'] != None and len(requestResults['preparingDownloads']) > 0:
                    payload = {'label': label}
                    moreDownloadUrls = sendRequest(serviceUrl + "download-retrieve", payload, apiKey)

                    print('moreDownloadUrls[available]:', moreDownloadUrls['available'])

                    downloadIds = []
                    sleep_count = 0

                    for download in moreDownloadUrls['available']:
                        downloadIds.append(download['downloadId'])
                        print("(line 163) DOWNLOAD: " + download['url'])

                    # Didn't get all of the requested downloads, call the download-retrieve method again after 30 seconds,
                    # but only do this 20 times (attempt for about 10 minutes) then give up
                    while len(downloadIds) < requestedDownloadsCount and sleep_count <= 20:
                        preparingDownloads = requestedDownloadsCount - len(downloadIds)
                        sleep_count += 1
                        print("\n", preparingDownloads, "downloads are not available. Waiting for 30 seconds.\n", "{}/20 attempts.\n".format(sleep_count))
                        time.sleep(30)
                        print("Trying to retrieve data\n")
                        moreDownloadUrls = sendRequest(serviceUrl + "download-retrieve", payload, apiKey)
                        for download in moreDownloadUrls['available']:
                            if download['downloadId'] not in downloadIds:
                                downloadIds.append(download['downloadId'])
                                print("(line 175) DOWNLOAD: " + download['url'])

                else:
                    # Get all available downloads
                    for download in requestResults['availableDownloads']:
                        # TODO :: Implement a downloading routine
                        print("(line 181) DOWNLOAD: " + download['url'])

                print("\nAll downloads are available to download.\n")
        else:
            print("Search found no results.\n")

    # Logout so the API Key cannot be used anymore
    endpoint = "logout"
    if sendRequest(serviceUrl + endpoint, None, apiKey) == None:
        print("Logged Out\n\n")
    else:
        print("Logout Failed\n\n")
