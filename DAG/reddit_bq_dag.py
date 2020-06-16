from datetime import datetime as dt, timedelta, date

from airflow import models, DAG

from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcPySparkOperator, \
    DataprocClusterDeleteOperator
# All needed to manage dataproc clusters

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# As the name implies, it allows the data to be transfered from GCS to BigQuery.
# In a traditional bash script, this could not be part of the DAG.

from airflow.models import Variable
# Used to read variable from environment such as gcs bucket, library, region, etc.

from airflow.utils.trigger_rule import TriggerRule

current = date.today()
yesterday = str(current - timedelta(days=1))

BUCKET = "gs://r_etl"

PYSPARK_JOB = BUCKET + "/spark_job/reddit-spark.py"

REDDIT_JOB = BUCKET + "/reddit_job/reddit_daily_load.py"

# The above two variables are examples of env variables that can be extracted by Variable

DEFAULT_DAG_ARGS = \
    {
    'owner': "airflow",
    'depends_on_past': False,
    'start_date': dt.utcnow(),
    'email_on_retry': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'project_id': "reddit-etl",
    "schedular_interval": "0 0 * * *"
    }

with DAG("reddit_submission_etl", default_args=DEFAULT_DAG_ARGS) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id="create_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        master_machine_type="n1-standard-1",
        worker_machine_type="n1-standard-1",
        num_workers=2,
        region="us-east1",
        zone="us-east1-b",
        init_actions_uris='gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh',
        metadata='PIP_PACKAGES=pandas praw google-cloud-storage'
    )

    submit_reddit = DataProcPySparkOperator(
        task_id="run_reddit_etl",
        main=REDDIT_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1"
    )

    bq_load_submissions = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_submissions",
        bucket="r_etl",
        source_objects=["submissions_store/" + yesterday + "*"],
        destination_project_dataset_table="reddit-etl.data_analysis.submissions",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    submit_pyspark = DataProcPySparkOperator(
        task_id="run_pyspark_etl",
        main=PYSPARK_JOB,
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1"
    )

    bq_load_analysis = GoogleCloudStorageToBigQueryOperator(
        task_id="bq_load_analysis",
        bucket="r_etl",
        source_objects=["spark_results/" + yesterday + "_calculations/part-*"],
        destination_project_dataset_table="reddit-etl.data_analysis.submission_analysis",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        skip_leading_rows=0,
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ds_nodash}}",
        region="us-east1",
        trigger_rule=TriggerRule.ALL_DONE
    )

    create_cluster.dag = dag

    create_cluster.set_downstream(submit_reddit)

    submit_reddit.set_downstream(bq_load_submissions)

    bq_load_submissions.set_downstream(submit_pyspark)

    submit_pyspark.set_downstream(bq_load_analysis)

    bq_load_analysis.set_downstream(delete_cluster)