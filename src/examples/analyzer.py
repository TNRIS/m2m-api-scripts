# find unique urls in returned downloadUrls list from usgs m2m api
from json import loads

def analyzer(d):
    with open(d, 'r') as file:
        for o in file['preparingDownloads']:
            print(o['url'])
    # unique = list(set(d))
    # print(unique)

if __name__ == "__main__":
    data = loads('preparing_downloads.json')
    print("running analyzer...")
    analyzer(data)
