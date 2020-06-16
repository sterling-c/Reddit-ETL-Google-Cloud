import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
import datetime as dt

# Reads Configuration file in order to
# get the bucket locations for the input and output.
bucket_source = "gs://r_etl/submissions_store/"
bucket_destination = "gs://r_etl/spark_results/"
# This gives us yesterday's date, which is when the data is based
# and how we will name the JSON files.
today = dt.date.today()
yesterday = today - dt.timedelta(days=1)
file_name = str(yesterday)

# Using spark to create a temporary table
# with the JSON file generated from Reddit's API.
# We will use this to create a new JSON file through querying.

sc = SparkContext()
spark = SQLContext(sc)
submission_data = spark.read.json(bucket_source+file_name+".json")
submission_data.registerTempTable("submission_data")

# This query selects some preexisting columns and creates two new ones.
# The first, upvote_category, assigns a category (should've been rank)
# based on the value of the submission's upvote_ratio.
# 1 is the highest category and 6 is the lowest.
# The second, comments_per_upvote, calculates what the name implies
# by dividing num_comment by score. Both values are integers so no type mismatch.

query = """
        select 
            id,
            title,
            date_created,
            score,
            upvote_ratio,
            case 
                when upvote_ratio between 0.9 and 1 then 1 
                when upvote_ratio between 0.8 and 0.8999 then 2
                when upvote_ratio between 0.7 and 0.7999 then 3
                when upvote_ratio between 0.6 and 0.6999 then 4 
                when upvote_ratio between 0.5 and 0.5999 then 5 
                when upvote_ratio between 0 and 0.4999 then 6 
            END upvote_category,
            num_comments,
            round((num_comments/score), 2) AS comments_per_upvote,
            permalink
        from 
            submission_data 
        """

submission_calculations = spark.sql(query)

# This creates a folder, based on yesterday's date, and saves the output there.
destination = bucket_destination+file_name+"_calculations"
submission_calculations.coalesce(1).write.format("json").save(destination)
