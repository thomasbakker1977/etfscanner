from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG: email a summary of the last two weeks of ETF prices, every two weeks
default_args = {
    'owner': 'etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id='etf_report_email',
    default_args=default_args,
    description='Send the ETF summary email every two weeks',
    # A cron expression cannot express a fortnightly cadence, so use a fixed
    # interval anchored on start_date: runs land every 14 days at 07:00 UTC.
    schedule_interval=timedelta(weeks=2),
    start_date=datetime(2026, 1, 5, 7, 0),
    catchup=False,
    max_active_runs=1,
) as dag:

    # Paths configured for fastapi-node01 deployment
    VENV_PY = '/home/fastapi/etfscanner_site/.venv/bin/python'
    REPO_DIR = '/home/fastapi/etfscanner_site'
    DB_FILE = f'{REPO_DIR}/etf_data.duckdb'
    TABLE = 'etf_prices'
    PERIOD_DAYS = 14

    report_cmd = (
        f"cd {REPO_DIR} && {VENV_PY} send_report_email.py "
        f"--db {DB_FILE} --table {TABLE} --period-days {PERIOD_DAYS}"
    )

    # No `env` override: the task inherits the worker environment, which must
    # supply EMAIL_FROM, EMAIL_TO and the SMTP_* settings (see README).
    send_report = BashOperator(
        task_id='send_biweekly_report_email',
        bash_command=report_cmd,
    )

    send_report
