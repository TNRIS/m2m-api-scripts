# usgs-m2m-api

A script to transfer data using the [USGS Machine-to-Machine (M2M) API](https://m2m.cr.usgs.gov/) to a specified S3 bucket and key. The Machine-to-Machine API is a JSON-based REST API used to interact with USGS/EROS data inventories.

This script was created for a specific purpose here at TNRIS to transfer NAIP 2020 imagery for the state of Texas in June 2021. Was written with the thought that it could be used again for other datasets if desired, or for future years of NAIP. An AWS EC2 (server) was used to run the script in the cloud and dump the data directly into a specified S3 bucket/key.

#### base-level requirements
- Ubuntu 20.04 LTS (Focal Fossa) or Linux Mint 20.1 (Ulyssa)
  - other OS's can be used but this documentation is specific to Debian-based Linux distributions.
- Python 3.8.5
- AWS Credentials
- USGS Machine-to-Machine (M2M) API Login Account and Request
  - request access at https://m2m.cr.usgs.gov/login and clicking the "Create New Account" button
  - once account is created, an Access Request for data can be created. Approval may take a day or two depending upon a variety of circumstances.

#### setup
- `git clone https://github.com/TNRIS/m2m-api-scripts.git`
- create a python virtual environment for the project (v3.8.5) using venv or whatever method you prefer
  - `python3 -m venv venv`
- activate the python virtual environment
  - `. venv/bin/activate`
- `cd m2m-api-scripts/src` & `pip install -r requirements.txt`
- make a copy or edit the name of the ./src/config/__set-env-secrets-example.sh__ file so you have just a __set-env-secrets.sh__ file in the same directory or wherever you want.
- edit the __set-env-secrets.sh__ file to include your specific USGS M2M credentials, as well as the S3 bucket and key information if you are going to save data to S3.
- `source ./src/config/set-env-secrets.sh` to get secrets as local environment variables which are required variables in the script(s).
- make necessary changes to script(s) for your specific purpose.
- `python <insert-script-name-you-want-to-run-here.py>` to run script

#### python requirements.txt
- boto3==1.17.87
- botocore==1.20.87
- certifi==2021.5.30
- chardet==4.0.0
- idna==2.10
- jmespath==0.10.0
- python-dateutil==2.8.1
- requests==2.25.1
- s3transfer==0.4.2
- six==1.16.0
- urllib3==1.26.5
