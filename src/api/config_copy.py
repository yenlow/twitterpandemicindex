# Config params
# Fill in with your credentials and rename file to config.py

import os, configparser

#for Google API (not req for public bigquery)
gspread_url = 'http://xxx'
credentials_google = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") #assuming you save keys as env variables

#for Github v3 API
credentials_github = os.environ.get("GITHUB_ACCESS_TOKEN") #assuming you save keys as env variables

def twitter_api():
    consumer_key = "xxx"
    consumer_secret = "xxx"
    access_token = "xxx"
    access_secret = "xxx"
    return consumer_key, consumer_secret, access_token, access_secret

def aws_api():
    region = 'us-west-1'
    access_key_id = 'xxx'
    access_secret = 'xxx'
    return region, access_key_id, access_secret

def ascend_api(profile="trial"):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser("~/.ascend/credentials"))
    access_key = config.get(profile, "ascend_access_key_id")
    secret_key = config.get(profile, "ascend_secret_access_key")
#    access_token = config.get(profile, "ascend_access_token")
    return access_key, secret_key