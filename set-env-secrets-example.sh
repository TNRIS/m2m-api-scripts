#!/usr/bin/env bash
# SET AWS S3 CREDS AS ENV VARIABLES
export AWS_KEY="<user aws iam access key>"
export AWS_SECRET_KEY="<user aws iam secret key>"
export S3_BUCKET="<s3 bucket where to upload>"
export S3_KEY="<s3 bucket key>"

# SET USGS M2M CREDS AS ENV VARIABLES
export M2M_USER="<usgs m2m api username>"
export M2M_PASS="<usgs m2m api password>"

echo "environment variables set!!!"
