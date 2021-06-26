# =============================================================================
#
# modified from USGS/EROS inventory service example found here:
# https://m2m.cr.usgs.gov/api/docs/examples/
# Python - JSON API
#
# June 2021
# script to transfer large amount of NAIP 2020 data to TNRIS s3 bucket
#
# imports======================================================================

import json, io, time
from zipfile import ZipFile
import requests
import sys, os
import time, csv
import boto3
from botocore.exceptions import ClientError

# =============================================================================

class M2MTransfer(object):
    def __init__(self, service_url, data_path, dataset_name, label, spatial_filter, temporal_filter, acquisition_filter):
        # setup variables from environment variables
        self.s3_bucket = os.environ['S3_BUCKET']
        self.s3_key = os.environ['S3_KEY']
        self.m2m_user = os.environ['M2M_USER']
        self.m2m_pass = os.environ['M2M_PASS']

        self.service_url = service_url
        self.data_path = data_path
        self.label = label
        self.dataset_name = dataset_name
        self.starting_num = 1
        self.page_count = 0
        self.scene_count = 0
        self.product_count = 0
        self.downloader_count = 0
        self.upload_count = 0
        self.jpeg_count = 0
        self.skip_count = 0

        self.spatial_filter = spatial_filter
        self.temporal_filter = temporal_filter
        self.acquisition_filter = acquisition_filter

    # send http request
    def send_request(self, url, data, api_key = None):
        # print('running send_request method')
        json_data = json.dumps(data)
        if api_key == None:
            response = requests.post(url, json_data)
        else:
            headers = {'X-Auth-Token': api_key}
            response = requests.post(url, json_data, headers = headers)
        # print('response:', response.__dict__['reason'])
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
            elif httpStatusCode == 429:
                print("429 Rate Limit")
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

    # method to upload file to s3
    def upload_file(self, file_name, bucket, object_name=None):
        # print('running upload_file method')
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

    # def rerun_list(self, list):
    #     print('rerun_list method')
    #     # check if any urls have been added to rerun list;
    #     # if so, try response again with downloader func
    #     if len(list) > 0:
    #         print("rerun_list has {} urls. preparing to run...".format(len(list)))
    #         time.sleep(3)
    #
    #         for u in list:
    #             response = requests.get(u, stream=True)
    #             if response.ok:
    #                 self.downloader(response, data_path)

    # method to unzip and remove local file from staging area
    # def unzipper(self, path, file_name):
    #     print('unzipper method')
    #     for file in os.listdir(path):
    #         if file.endswith('.ZIP') or file.endswith('.zip'):
    #             print("found zip:", file)
    #             with ZipFile(path + file, 'r') as z:
    #                 print('extracting {}...'.format(z))
    #                 z.extractall(path)
    #             os.remove(path + file)
    #     # call uploader func
    #     self.uploader(path, file_name)

    # method to upload file to s3; uses upload_file method
    def uploader(self, path, file_name):
        print('running uploader method')
        for file in os.listdir(path):
            if file.endswith('.jp2'):
                # upload jp2 file to s3
                with open(path + file, 'rb'):
                    # print('upload path + file_name', path + file)
                    self.upload_file(path + file, self.s3_bucket, object_name=self.s3_key+file)
                    self.upload_count += 1
                os.remove(path + file)

    # method to setup file_name and download file if jp2; calls uploader method
    def downloader(self, r):
        print('running downloader method')
        self.downloader_count += 1
        file_name = r.headers['Content-Disposition'].rsplit('=')[1].strip('""')
        print('file_name =', file_name)
        # write file from url to local file
        if file_name.endswith('.jp2'):
            print('found a jp2')
            with open(self.data_path + file_name, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            self.jpeg_count += 1
            # if jp2 file, call uploader method
            self.uploader(self.data_path, file_name)
            # when downloader_count == product_count,
            # run paginator to search for more pages of data
            if self.downloader_count == self.product_count:
                print('downloader_count: {} --- product_count: {}'.format(self.downloader_count, self.scene_count))
                self.paginator(self.scene_payload, self.scenes)
        elif file_name.endswith('.ZIP') or file_name.endswith('.zip'):
            print('skipping zip file')
            self.skip_count += 1
            if self.downloader_count == self.product_count:
                print('downloader_count: {} --- product_count: {}'.format(self.downloader_count, self.scene_count))
                self.paginator(self.scene_payload, self.scenes)
        else:
            print('found different file type:', file_name)
            self.skip_count += 1

    def download_retrieve(self, count, request):
        print('running download_retrieve method')
        # PreparingDownloads has a valid link that can be used but data may not be immediately available
        # Call the download-retrieve method to get download that is available for immediate download
        if request['preparingDownloads'] != None and len(request['preparingDownloads']) > 0:
            payload = {'label': self.label}
            retrieve = self.send_request(self.service_url + "download-retrieve", payload, self.api_key)
            # print('retrieve response:', retrieve)
            download_urls = []
            sleep_count = 0

            for download in retrieve['available']:
                # print('adding retrieve[available] url:', download['url'])
                download_urls.append(download['url'])

            # print("count is: {} VS download_urls length: {}".format(count, len(download_urls)))

            # if didn't get all of the requested downloads, call the download-retrieve method again after 5 seconds
            while len(download_urls) < count:
                preparing = count - len(download_urls)
                sleep_count += 1
                print(preparing, "downloads are not available. waiting for 10 seconds...\n")
                available_length = len(retrieve['available'])
                time.sleep(10)
                print("Attempting to retrieve data...\n")
                retrieve = self.send_request(self.service_url + "download-retrieve", payload, self.api_key)
                # if available_length < len(retrieve['available']):
                #     print('found some new data available')
                # go ahead and search retrieve['requested'] for urls not yet added
                for download in retrieve['available']:
                    if download['url'] not in download_urls:
                        download_urls.append(download['url'])

            # print("download_urls COUNT:", len(download_urls))
            # print("FINAL download_urls LIST:", download_urls)

            count = 0
            # rerun_list = []
            # start download, unzip, and upload processes
            # calls the downloader function above at the start
            for url in download_urls:
                count += 1
                print('{}---{}'.format(count, url))
                response = requests.get(url, stream=True)
                if response.ok:
                    print('response OK')
                    self.downloader(response)
                else:
                    print('response NO good')
                    # rerun_list.append(url)

        else:
            # Get all available downloads
            for download in request['availableDownloads']:
                # TODO :: Implement a downloading routine
                print("DOWNLOAD: " + download['url'])

    def download_request(self, downloads):
        print('running download_request method')
        # Did we find products?
        if downloads:
            downloads_count = len(downloads)
            print('REQUESTED DOWNLOADS COUNT:', downloads_count)
            payload = {'downloads': downloads, 'label': self.label}
            # Call the download to get the direct download urls
            download_request = self.send_request(self.service_url + "download-request", payload, self.api_key)

        # call download_retrieve method
        self.download_retrieve(downloads_count, download_request)

    def download_options(self, ids):
        print('running download_options method')
        # Find the download options for these scenes
        # NOTE :: Remember the scene list cannot exceed 50,000 items!
        payload = {'datasetName': self.dataset['datasetAlias'], 'entityIds': ids}
        download_options = self.send_request(self.service_url + "download-options", payload, self.api_key)
        # print('download options:', download_options)
        # Aggregate a list of available products
        downloads = []
        for product in download_options:
            # Make sure the product is available for this scene
            if product['available'] == True:
                self.product_count += 1
                downloads.append({'entityId': product['entityId'], 'productId': product['id']})

        print('(line 259) downloads list length --- {}'.format(len(downloads)))
        print('(line 260) product_count --- {}'.format(self.product_count))

        # call download_request method
        self.download_request(downloads)

    def paginator(self, payload, scenes):
        # for pagination; add scenes['nextRecord'] as payload['startingNumber']
        print('running paginator method')
        if self.scenes['nextRecord'] and self.page_count > 0:
            scene_ids = []
            self.page_count += 1
            print('self.page_count =', self.page_count)
            self.starting_num = self.scenes['nextRecord']
            self.scene_payload = {
                'datasetName': self.dataset['datasetAlias'],
                # 'maxResults': 3,
                'startingNumber': self.starting_num,
                'sceneFilter': {
                    'spatialFilter': self.spatial_filter,
                    'acquisitionFilter': self.temporal_filter
                    }
                }
            print('new starting_num in scene payload:', self.scene_payload)
            self.scenes = self.send_request(self.service_url + "scene-search", self.scene_payload, self.api_key)
            for result in self.scenes['results']:
                scene_ids.append(result['entityId'])
            for e in scene_ids:
                self.scene_count += 1
            # when finished compiling scene ids, print out total number of scenes
            # to compare with scenes['totalHits']
            print('{} of {} scene_ids built.\n'.format(self.scene_count, self.total_hits))
            # call download_options method
            self.download_options(scene_ids)

        else:
            print('\nNo more pagination!\n')
            print('TOTAL PRODUCTS:', self.product_count)
            print('TOTAL SKIPPED PRODUCTS:', self.skip_count)
            print('TOTAL DOWNLOADED JP2s:', self.jpeg_count)
            print('TOTAL UPLOADED JP2s:', self.upload_count)
            self.logout()

    def scene_search(self):
        print('running scene_search method')
        # set more or remove filters here; for testing, setting the maxResults filter may be useful
        print('self.dataset["datasetAlias"]:', self.dataset['datasetAlias'])
        self.scene_payload = {
            'datasetName': self.dataset['datasetAlias'],
            # 'maxResults': 3,
            'startingNumber': self.starting_num,
            'sceneFilter': {
                'spatialFilter': self.spatial_filter,
                'acquisitionFilter': self.temporal_filter
                }
            }
        # Now I need to run a scene search to find data to download
        print("Searching scenes...\n")
        self.scenes = self.send_request(self.service_url + "scene-search", self.scene_payload, self.api_key)
        # print('print initial scenes response:', self.scenes)
        self.total_hits = self.scenes['totalHits']
        print('scenes request total hits:', self.total_hits, '\n')

        # Did we find anything?
        if self.scenes['recordsReturned'] > 0:
            # add to page_count
            self.page_count += 1
            # Aggregate a list of scene ids
            scene_ids = []
            for result in self.scenes['results']:
                # Add scene to list
                scene_ids.append(result['entityId'])

            for e in scene_ids:
                self.scene_count += 1
            # when finished compiling scene ids, print out total number of scenes
            # to compare with scenes['totalHits']
            print('{} of {} scene_ids built.\n'.format(self.scene_count, self.total_hits))
            # call download_options method
            self.download_options(scene_ids)
        else:
            print("Search found no results.\n")

    def dataset_searcher(self):
        print('running dataset_searcher method')
        payload = {'datasetName': self.dataset_name, 'spatialFilter': self.spatial_filter, 'temporalFilter': self.temporal_filter}

        print("Searching datasets...\n")
        datasets = self.send_request(self.service_url + "dataset-search", payload, self.api_key)
        if datasets:
            print("Found datasets:", len(datasets))
        # download datasets
        for dataset in datasets:
            # skip any other datasets that might be found, logging it in case I want to look into it later
            if dataset['datasetAlias'] == self.dataset_name:
                # print("Found dataset " + dataset['collectionName'] + " but skipping it.\n")
                self.dataset = dataset
                # call scene_search method
                self.scene_search()

    def login(self):
        print('running login method')
        # login
        payload = {'username': self.m2m_user, 'password': self.m2m_pass}
        self.api_key = self.send_request(self.service_url + "login", payload)
        print("API Key: " + self.api_key + "\n")
        # call dataset_searcher
        self.dataset_searcher()

    def logout(self):
        print('running logout method')
        # Logout so the API Key cannot be used anymore
        endpoint = "logout"
        if self.send_request(self.service_url + endpoint, None, self.api_key) == None:
            print("Logged Out\n\n")
        else:
            print("Logout Failed\n\n")


if __name__ == "__main__":
    #
    # CHANGE THE VALUES BELOW AS NEEDED
    #
    # set data service url; shouldn't need to be changed but maybe
    service_url = "https://m2m.cr.usgs.gov/api/api/json/stable/"
    # set relative data_path for staging; this is used for staging data
    # during the process and nothing will be saved here permanently
    data_path = "../data/"
    # set dataset_name to find the data you want
    dataset_name = "naip"
    # set label name
    label = "Texas NAIP 2020"
    # set texas bbox as spatialFilter; change/adjust as necessary
    spatial_filter =  {
        'filterType': "mbr",
        'lowerLeft': {'latitude': 25, 'longitude': -107},
        'upperRight': {'latitude': 37, 'longitude': -93}
        }
    # set temporalFilter; usually same or similar as acquisitionFilter
    temporal_filter = {'start': '2020-01-01', 'end': '2021-06-01'}
    # set acquisitionFilter
    acquisition_filter = {'end': '2021-06-01', 'start': '2020-01-01'}

    print("\nStarting M2MTransfer script...\n")
    time.sleep(2)
    print("\nLogging in...\n")

    # instantiate and fire login method to begin script
    transfer = M2MTransfer(service_url, data_path, dataset_name, label, spatial_filter, temporal_filter, acquisition_filter)
    transfer.login()
