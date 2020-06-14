#! usr/bin/env python3
import praw
import pandas as pd
import datetime as dt
import configparser

# This goes into a configuration file
# and retrieves reddit credentials to
# access the Reddit API. Also gets the
# locatin of the GCP bucket where we
# will save the end product
cfg = configparser.ConfigParser()
cfg.read('Configuration/Config.cfg')
c_id = cfg.get("Reddit", "client_id")
c_s = cfg.get("Reddit", "client_secret")
u_a = cfg.get("Reddit", "user_agent")
user = cfg.get("Reddit", "username")
pswd = cfg.get("Reddit", "password")
bucket_loc = cfg.get("GCP", "bucket_location")

today = dt.date.today()
yesterday = today - dt.timedelta(days=1)

reddit = praw.Reddit(client_id=c_id,
                     client_secret=c_s,
                     password=pswd,
                     user_agent=u_a,
                     username=user)

# Selects the subreddit that we will pull submissions from.
subreddit = reddit.subreddit('all')

# This dictionary's key/value pairs matches the schema of the BigQuery Table
# that this information will evetually be uploaded into.
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

# We create a for-loop to insert the each attribute of each
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

# We convert the dictionary into a Pandas Dataframe,
# change the format of the "date_created field,
# and then convert the dataframe into a JSON file saved in the GCP bucket
posts_frame = pd.DataFrame(posts)
posts_frame["date_created"] = pd.to_datetime(posts_frame["date_created"],unit='s')
posts_frame["date_created"] = posts_frame["date_created"].dt.strftime('%Y-%m-%d')
posts_frame.to_json(bucket_loc + str(yesterday) + '.json', default_handler=str, orient='records', lines=True)