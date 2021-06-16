This directory is used for testing hitting the API and getting returned JSON data to figure out how the API is organized. You can also run tests with your scripts to actually download the spatial data you want from the USGS, but be careful when doing this and make sure you have a max return limit set so you don't take up all the space on your computer.

The `s3_uploader.py` script uses this directory as a staging area before uploading the files to s3. 
