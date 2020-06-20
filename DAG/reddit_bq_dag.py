from datetime import datetime as dt, timedelta, date
from airflow import models, DAG
from airflow.contrib.operators.dataproc_operator import DataProcPySparkOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

#This gives us yesterday's date
current = date.today()
yesterday = str(current - timedelta(days=1))

# These are the paths for the reddit job and the pyspark job.
BUCKET = "BUCKET/PATH"
PYSPARK_JOB = BUCKET + "PATH/TO/PYSPARK/FILE"
REDDIT_JOB = BUCKET + "PATH/TO/REDDIT/FILE"
CLUSTER = "CLUSTER_NAME"
REGION = "REGION"
PROJECT_ID = "PROJECT_ID"

DEFAULT_DAG_ARGS = \
    {
        'owner': "airflow",
        'depends_on_past': False,
        "start_date": dt(2020, 6, 19),
        "email_on_retry": False,
        "email_on_failure": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "project_id": PROJECT_ID
    }

with DAG("reddit_submission_etl", default_args=DEFAULT_DAG_ARGS, catchup=False, schedule_interval="0 0 * * *") as dag:

# Extracts Reddit Posts from r/all and saves the results as a JSON file.
    submit_reddit = DataProcPySparkOperator(
        task_id="run_reddit_etl",
        main=REDDIT_JOB,
        cluster_name=CLUSTER,
        region=REGION
    )

# Inserts the JSON files generated by the task "run_reddit_etl" into the regular table.
    bq_load_submissions = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_submissions",
        bucket=BUCKET,
        source_objects=["SUBMISSION/FOLDER" + yesterday + "*"],
        destination_project_dataset_table="LOCATION.OF.TABLE",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

# Using the JSON files generated by "run_reddit_etl", this reprocesses the data
# and performs new calculations. Saves new JSON files in a new file.
    submit_pyspark = DataProcPySparkOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_JOB,
        cluster_name=CLUSTER,
        region=REGION
    )

# An error occurs when BigQuery attempts to insert
# "comments_per_upvote" into the table.
# When autodetect=True, BigQuery struggles to convert
# the data to float or numeric.
# Changing autodetect=False and defining schema_fields
# forces the data type conversion and solves the problem.
    bq_load_analysis = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_analysis",
        bucket=BUCKET,
        source_objects=["SPARK/OUTPUT/FOLDER" + yesterday + "_calculations/part-*"],
        destination_project_dataset_table="LOCATION.OF.TABLE",
        autodetect=False,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
        schema_fields= [
        	{
        		"mode": "NULLABLE",
    			"name": "id",
    			"type": "STRING"
        	},
        	{
        		"mode": "NULLABLE",
    		  	"name": "title",
    		  	"type": "STRING"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name": "date_created",
    		    "type": "DATE"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name":  "score",
    		    "type": "INTEGER"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name": "upvote_ratio",
    		    "type": "FLOAT"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name":  "upvote_category",
    		    "type": "INTEGER"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name": "num_comments",
    		    "type": "INTEGER"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name":  "comments_per_upvote",
    		    "type": "NUMERIC"
    		},
    		{
    		    "mode": "NULLABLE",
    		    "name": "permalink",
    		    "type": "STRING"
    		}
        ]
    )

    submit_reddit.dag = dag

    submit_reddit.set_downstream(bq_load_submissions)

    bq_load_submissions.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream(bq_load_analysis)
