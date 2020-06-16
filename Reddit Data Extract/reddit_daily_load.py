#! usr/bin/env python3
from google.cloud import storage
import praw
import pandas as pd
import datetime as dt

# This gives us the the date of yesterday
today = dt.date.today()
yesterday = today - dt.timedelta(days=1)

# This gives us access to the Reddit API.
# Getting access through PRAW (Python Reddit API Wrapper),
# we need to register for access to the API and create an app.
# Normally, I would never hard-code this information into the code,
# but for simplicity's sake, I will do it here for now.
reddit = praw.Reddit(client_id="$REDDIT_APP_ID",
                     client_secret="$SECRET_KEY",
                     password="$REDDIT_PASSWORD",
                     user_agent="$REDDIT_APP_NAME",
                     username="$REDDIT_USERNAME")

# This gives us the name of the file created by this script
# and gives the path of the file within the destination bucket.
# The resulting file is named after the date of the data was collected.
bucket_loc = "submissions_store/" + str(yesterday) + ".json"

# Selects the subreddit that we will pull submissions from.
# 'all' isn't a real subreddit,
# but an aggregate of the popular posts from any subreddit at the time.
subreddit = reddit.subreddit('all')

# This dictionary's key/value pairs matches the schema of the BigQuery Table
# that this information will eventually be uploaded into.
posts = {"id": [],
         "title": [],
         "name": [],
         "date_created": [],
         "subreddit": [],
         "score": [],
         "upvote_ratio": [],
         "num_comments": [],
         "url": [],
         "permalink": []
         }

# We create a for-loop to insert the each desired attribute of each
# post to the corresponding key.
for submission in subreddit.top(time_filter='day'):
    posts["id"].append(submission.id)
    posts["title"].append(submission.title)
    posts["name"].append(submission.name)
    posts["date_created"].append(submission.created_utc)
    posts["subreddit"].append(submission.subreddit)
    posts["score"].append(submission.score)
    posts["upvote_ratio"].append(submission.upvote_ratio)
    posts["num_comments"].append(submission.num_comments)
    posts["url"].append(submission.url)
    posts["permalink"].append(submission.permalink)

# We convert the dictionary into a Pandas Dataframe
# and change the format of the "date_created" field.
posts_frame = pd.DataFrame(posts)
posts_frame["date_created"] = pd.to_datetime(posts_frame["date_created"], unit='s')
posts_frame["date_created"] = posts_frame["date_created"].dt.strftime('%Y-%m-%d')

# The following lines will give us access to the Google Cloud Storage.

# This line instantiates a client accessing the storage client.
storage_client = storage.Client()

# This line gives us access to the bucket used for our project.
# You need to provide the name or id of the bucket you are using.
bucket = storage_client.get_bucket("$BUCKET_NAME")

# This line creates the JSON file
# and opens it up for reading and writing.
blob = bucket.blob(bucket_loc)

# This line converts the dataframe we created into a JSON data
# and writes that data into our JSON file.
blob.upload_from_string(data=posts_frame.to_json(default_handler=str, orient='records', lines=True),
                        content_type='application/json'
                        )
