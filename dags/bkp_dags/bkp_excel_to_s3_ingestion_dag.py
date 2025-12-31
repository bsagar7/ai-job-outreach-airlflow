# from airflow import DAG
# from airflow.operators.python import PythonOperator, ShortCircuitOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.utils.log.logging_mixin import LoggingMixin
# from datetime import datetime
# import pandas as pd
# import json

# BUCKET_NAME = "ai-job-outreach-app"
# PROCESSED_KEY = "processed/jobs/jobs_parsed.json"

# log = LoggingMixin().log


# def read_and_validate_excel():
#     df = pd.read_excel("/opt/airflow/dags/sample_job_applications.xlsx")
#     print(df.columns)
#     required_cols = {
#         "job_id",
#         "company",
#         "role",
#         "manager_name",
#         "manager_email",
#         "job_description"
#     }

#     if not required_cols.issubset(df.columns):
#         raise ValueError("Schema validation failed")

#     records = df.to_dict(orient="records")

#     log.info(json.dumps({
#         "event": "excel_read",
#         "records_count": len(records),
#         "status": "SUCCESS"
#     }))

#     return records


# def check_if_already_processed():
#     s3 = S3Hook(aws_conn_id="aws_default")
#     exists = s3.check_for_key(PROCESSED_KEY, BUCKET_NAME)

#     log.info(json.dumps({
#         "event": "idempotency_check",
#         "processed_exists": exists
#     }))

#     return not exists   # True = continue, False = stop DAG


# def upload_to_s3(**context):
#     records = context["ti"].xcom_pull(task_ids="read_excel")

#     s3 = S3Hook(aws_conn_id="aws_default")

#     s3.load_string(
#         string_data=json.dumps(records, indent=2),
#         key=PROCESSED_KEY,
#         bucket_name=BUCKET_NAME,
#         replace=True
#     )

#     log.info(json.dumps({
#         "event": "s3_upload",
#         "bucket": BUCKET_NAME,
#         "key": PROCESSED_KEY,
#         "records_uploaded": len(records),
#         "status": "SUCCESS"
#     }))


# with DAG(
#     dag_id="excel_to_s3_ingestion",
#     start_date=datetime(2025, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["s3", "ingestion", "idempotent"]
# ):

#     check_processed = ShortCircuitOperator(
#         task_id="check_if_already_processed",
#         python_callable=check_if_already_processed
#     )

#     read_excel = PythonOperator(
#         task_id="read_excel",
#         python_callable=read_and_validate_excel
#     )

#     upload_s3 = PythonOperator(
#         task_id="upload_to_s3",
#         python_callable=upload_to_s3,
#         provide_context=True
#     )

#     check_processed >> read_excel >> upload_s3
