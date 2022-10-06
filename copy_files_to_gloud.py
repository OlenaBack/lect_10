from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

default_dag_args = {
    'email_on_failure': False,
    'retries': 0,
}

with DAG(
        dag_id="sales_upload_finally",
        schedule_interval="@daily",
        start_date=datetime(2022, 8, 1),
        end_date=datetime(2022, 8, 2),
        max_active_runs=1,
        default_args=default_dag_args,
        catchup=True
) as dag:
    gcp_operator = LocalFilesystemToGCSOperator(
        task_id='gcp_task',
        src='./dags/sales/{{ds}}/*',
        dst='src1/sales/v1/year={{ execution_date.year }}/month={{ "{:02}".format(execution_date.month) }}/day={{ "{:02}".format(execution_date.day) }}/',
        bucket='lect_10_ob',
        gcp_conn_id='google_cloud_default',
        mime_type='Folder',
        dag=dag
    )
