from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.log.logging_mixin import LoggingMixin
from io import BytesIO
from airflow.operators.email import EmailOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime
import pandas as pd
import json
import smtplib
import ssl
from email.message import EmailMessage
import os



BUCKET_NAME = "ai-job-outreach-app"
RAW_KEY = "raw/jobs_excel/job_applications.xlsx"
PROCESSED_KEY = "processed/jobs/jobs_parsed.json"

log = LoggingMixin().log


def check_if_raw_file_exists():
    s3 = S3Hook(aws_conn_id="aws_default")
    exists = s3.check_for_key(RAW_KEY, BUCKET_NAME)

    log.info({
        "event": "raw_file_check",
        "raw_file_exists": exists
    })

    return exists  # True = continue, False = stop DAG


def read_and_validate_excel():
    s3 = S3Hook(aws_conn_id="aws_default")

    # Read Excel file from S3 into memory
    obj = s3.get_key(key=RAW_KEY, bucket_name=BUCKET_NAME)
    file_bytes = obj.get()["Body"].read()

    df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")

    # Normalize columns
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    required_cols = {
        "job_id",
        "company",
        "role",
        "manager_name",
        "manager_email",
        "job_description"
    }

    if not required_cols.issubset(df.columns):
        raise ValueError("Schema validation failed")

    records = df.to_dict(orient="records")

    log.info({
        "event": "excel_read_success",
        "records_count": len(records)
    })

    return records

def generate_and_store_emails(**context):
    records = context["ti"].xcom_pull(task_ids="read_excel")

    s3 = S3Hook(aws_conn_id="aws_default")

    prompt_template = s3.read_key(
        key="prompts/cold_email_prompt.txt",
        bucket_name=BUCKET_NAME
    )

    generated = []
    now_ts = datetime.utcnow().isoformat()
    today = datetime.utcnow().date().isoformat()

    for r in records:
        prompt = (
            prompt_template
            .replace("{{company}}", r["company"])
            .replace("{{role}}", r["role"])
            .replace("{{manager_name}}", r["manager_name"])
            .replace("{{job_description}}", r["job_description"])
        )

        email_body = f"{prompt}"

        generated.append({
            **r,
            "email_body": email_body,
            "generated_at": now_ts
        })

    key = f"generated_emails/{today}_emails.json"

    s3.load_string(
        string_data=json.dumps(generated, indent=2),
        bucket_name=BUCKET_NAME,
        key=key, 
        replace=True
    )

    log.info({
        "event": "emails_generated_and_stored",
        "count": len(generated),
        "s3_key": key
    })

    return key



def send_and_audit_emails(**context):
    s3 = S3Hook(aws_conn_id="aws_default")

    generated_key = context["ti"].xcom_pull(
        task_ids="generate_and_store_emails"
    )

    emails = json.loads(
        s3.read_key(generated_key, BUCKET_NAME)
    )

    resume_bytes = (
        s3.get_key("assets/resume.pdf", BUCKET_NAME)
        .get()["Body"]
        .read()
    )

    SMTP_HOST = os.environ["AIRFLOW__SMTP__SMTP_HOST"]
    SMTP_PORT = int(os.environ["AIRFLOW__SMTP__SMTP_PORT"])
    SMTP_USER = os.environ["AIRFLOW__SMTP__SMTP_USER"]
    SMTP_PASSWORD = os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"]

    today = datetime.utcnow().date().isoformat()
    now_ts = datetime.utcnow().isoformat()

    master_key = "audit/master/jobs_master.json"

    if s3.check_for_key(master_key, BUCKET_NAME):
        master_data = json.loads(s3.read_key(master_key, BUCKET_NAME))
    else:
        master_data = []

    daily_results = []

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)

        for e in emails:
            status = "SENT"
            error = None

            try:
                msg = EmailMessage()
                msg["From"] = SMTP_USER
                msg["To"] = e["manager_email"]
                # msg["Subject"] = f"Interest in {e['company']} – {e['job_id']}"
                msg["Subject"] = f"{e['role']} Application at {e['company']}"

                msg.set_content(e["email_body"])

                msg.add_attachment(
                    resume_bytes,
                    maintype="application",
                    subtype="pdf",
                    filename="resume.pdf"
                )

                server.send_message(msg)

            except Exception as ex:
                status = "FAILED"
                error = str(ex)
                log.error(f"Email failed for {e['manager_email']}: {error}")

            audit_record = {
                **e,
                "status": status,
                "error": error,
                "processed_date": today,
                "processed_at": now_ts
            }

            daily_results.append(audit_record)
            master_data.append(audit_record)

    daily_key = f"audit/daily/{today}_jobs_processed.json"

    s3.load_string(
        string_data=json.dumps(daily_results, indent=2),
        bucket_name=BUCKET_NAME,
        key=daily_key,
        replace=True
    )

    s3.load_string(
        string_data=json.dumps(master_data, indent=2),
        bucket_name=BUCKET_NAME,
        key=master_key,
        replace=True
    )

    log.info({
        "event": "emails_sent_and_audited",
        "total": len(daily_results),
        "sent": sum(1 for r in daily_results if r["status"] == "SENT"),
        "failed": sum(1 for r in daily_results if r["status"] == "FAILED")
    })


def archive_raw_file():
    s3 = S3Hook(aws_conn_id="aws_default")

    archive_key = (
        f"archive/job_excel/"
        f"{datetime.utcnow().date()}_job_applications.xlsx"
    )

    # COPY raw → archive
    s3.copy_object(
        source_bucket_name=BUCKET_NAME,
        source_bucket_key=RAW_KEY,
        dest_bucket_name=BUCKET_NAME,
        dest_bucket_key=archive_key,
    )

    # DELETE original raw file
    s3.delete_objects(
        bucket=BUCKET_NAME,
        keys=[RAW_KEY]
    )

    log.info({
        "event": "file_archived",
        "archive_key": archive_key
    })


with DAG(
    dag_id="cold_email_outreach_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["s3", "ingestion", "idempotent"]
):

    check_processed = ShortCircuitOperator(
        task_id="check_if_raw_file_exists",
        python_callable=check_if_raw_file_exists
    )

    read_excel = PythonOperator(
        task_id="read_excel",
        python_callable=read_and_validate_excel
    )

    generate_and_store_emails = PythonOperator(
        task_id="generate_and_store_emails",
        python_callable=generate_and_store_emails,
        provide_context=True
    )

    send_and_audit_emails = PythonOperator(
        task_id="send_and_audit_emails",
        python_callable=send_and_audit_emails,
        provide_context=True
    )

    archive_raw_file = PythonOperator(
        task_id="archive_raw_file",
        python_callable=archive_raw_file
    )

    (
        check_processed
        >> read_excel
        >> generate_and_store_emails
        >> send_and_audit_emails
        >> archive_raw_file
    )
