# Reddit-ETL-Google-Cloud
This project creates an ETL pipeline that accesses Reddit's API to extract posts on Reddit and process them on Google Cloud Platform.


## Procedure
  1. Connect to pre-existing Dataproc Cluster with all required Python libraries already installed. These libraries are pandas, PRAW (Python Reddit API Wrapper), and Google Cloud Storage.
  2. Run the Reddit job. This connects to the Reddit API, pulls submissions from the subreddit 'r/all', converts the data into a dataframe to be changed, and finally save the results as a JSON file to a bucket in Google Cloud Storage.
  3. The data resulting from the previous job is inserted into a pre-existing BigQuery Table. 
  4. Take the data from the reddit job and process it through Spark. We take the data and create a simulated SQL table, query it with two new fields based on certain calculations, and then create and store a new JSON file. 
  5. Take the new JSON file from the previous step and place it into a special table.
  
  
## Python Scripts
  
### reddit_daily_load.py
Dependencies:
- google.cloud
  - Used to access the appropriate bucket in Google Cloud Storage and save JSON files to it
- praw
  - Used to access and extract data from the Reddit API
- pandas
  - Used to convert Reddit data into a JSON file
- datetime
  - Used for naming files 
  
Input:
- Reddit API credentials
  - Reddit Username
  - Reddit Password
  - Reddit App Name
  - Reddit App ID
  - Reddit Secret Key
- Google Cloud Storage bucket path
  
Output:
- JSON file containing Reddit Submission data. Schema is designed exactly to match the BigQuery table the data will be inserted into.
  
  
### reddit_spark.py
Dependencies:
- pyspark
  - Used to create a simulated table from the submission data created by reddit_daily_load.py
  - Query the table to select a few pre-existing fields and create two new fields through simple analysis and calculations
  - Reconvert that table into a new JSON file
- datetime
  - Used to retrieve the input file and name the output file
  
Input:
- Full path of the input files
- Full path for the output folder
  
Output:
- JSON file containing transformed data for Reddit submissions. Schema is designed exactly to match the BigQuery table the data will be inserted into.
  
## BigQuery Tables
  Both tables are partitioned by "date_created"
### submissions
Schema:
  - id
  - title
  - name
  - date_created
  - subreddit
  - score
  - upvote_ratio
  - num_comments
  - url
  - permalink
    
### submissions_analysis
Schema:
  - id
  - title
  - date_created
  - score
  - upvote_ratio
  - **upvote_category**
    - Gives a ranking based on the range of the upvote ratio from 1-6. 
  - num_comments
  - **comments_per_upvote**
    - Divides num_comments by score
  - permalink
