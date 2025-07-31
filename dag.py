from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.hooks.dataproc import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataproc import DataflowJobStatusSensor
from airflow.providers.google.cloud.operators.dataproc import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
import re
import os
import sys
import time
import pendulum
from math import ceil
from datetime import timedelta

script_dir = os.path.dirname(__file__)
util_dir = os.path.join(script_dir, '..', 'utils')
sys.path.append(util_dir)

import common_utilities as cu

src_config_file = "healthgrade_monthly_ingest_dependency.yaml"
env_config_file = "cp_config.yaml"
srcsys_config = cu.call_config_yaml(src_config_file)
timezone = pendulum.timezone("US/Central")

# Dataflow configuration
PROJECT_ID = Variable.get('GCP_PROJECT_ID', 'your-project-id')
REGION = Variable.get('DATAFLOW_REGION', 'us-central1')
GCS_STAGING_BUCKET = Variable.get('DATAFLOW_STAGING_BUCKET', 'your-staging-bucket')

default_args = {
    "owner": "hca_cp_autos",
    "depends_on_past": False,
    "start_date": pendulum.datetime(2022, 3, 1, tz=timezone),
    "email_on_success": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=480),
}

def create_dag(
    dag_id,
    schedule,
    start_date,
    source_system,
    frequency
):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        start_date=start_date,
        schedule_interval=schedule,
        catchup=False,
        max_active_runs=1,
        max_active_tasks=10,
        is_paused_upon_creation=True,
        template_searchpath="/home/airflow/gcs/dags/{lob}/sql/",
        tags=[f"{source_system}", f"{frequency}", "ingest", "healthgrade"]
    )
    with dag:
        start_job = DummyOperator(task_id="start_dag")
        end_job = DummyOperator(task_id="end_dag")

        now = pendulum.now(timezone)
        file_date = str(now.year) + str(now.month).zfill(2) + str(now.day).zfill(2)

        # Build Dataflow job parameters from your existing config
        def build_sftp_dataflow_parameters(**context):
            """Build parameters for the SFTP Dataflow job using existing config."""
            beam_parameters = [
                f'--project_id={PROJECT_ID}',
                f'--gcs_bucket={Variable.get("GCS_BUCKET")}',
                f'--sftp_host={srcsys_config.get("host", "")}',
                f'--sftp_port={srcsys_config.get("port_number", 22)}',
                f'--sftp_username={srcsys_config.get("user_name", "")}',
                f'--sftp_password={srcsys_config.get("password", "")}',
                f'--source_system={srcsys_config["sourcesysnm"]}',
                f'--remote_directory={srcsys_config["filelist"][0]["remote_directory"]}',
                f'--base_file_name={srcsys_config["filelist"][0]["base_file_name"]}',
                f'--file_extension={srcsys_config["filelist"][0]["file_extension"]}',
            ]
            return beam_parameters

        # Replace BashOperator with DataflowCreatePythonJobOperator
        sftp_get_files = DataflowCreatePythonJobOperator(
            task_id="sftp_get_files",
            py_file=f'gs://{GCS_STAGING_BUCKET}/dataflow/sftp_beam_pipeline.py',
            job_name=f'sftp-healthgrade-{frequency}-' + '{{ ds_nodash }}-{{ ts_nodash }}',
            py_options=build_sftp_dataflow_parameters(),
            dataflow_config={
                'project_id': PROJECT_ID,
                'location': REGION,
                'staging_location': f'gs://{GCS_STAGING_BUCKET}/staging/',
                'temp_location': f'gs://{GCS_STAGING_BUCKET}/temp/',
                'max_workers': 5,
                'num_workers': 2,
                'machine_type': 'n1-standard-2',
                'disk_size_gb': 50,
                'use_public_ips': False,
            },
            py_requirements=[
                'apache-beam[gcp]==2.53.0',
                'paramiko>=3.0.0',
                'google-cloud-storage>=2.10.0',
                'pendulum>=2.0.0'
            ],
            retries=1,
            retry_delay=timedelta(minutes=10),
            dag=dag,
        )

        # Monitor SFTP Dataflow job completion
        wait_for_sftp_job_done = DataflowJobStatusSensor(
            task_id="wait_for_sftp_job_done",
            job_id="{{ task_instance.xcom_pull(task_ids='sftp_get_files', key='job_id') }}",
            expected_statuses=(DataflowJobStatus.JOB_STATE_DONE,),
            project_id=PROJECT_ID,
            location=REGION,
            mode="reschedule",
            poke_interval=60,  # Check every minute
            timeout=60*30,     # 30 minute timeout
            dag=dag,
        )

        start_job >> sftp_get_files >> wait_for_sftp_job_done

        # run parallel df jobs - keeping your existing logic
        with TaskGroup(group_id="run_dataflow_jobs") as tg1:
            with TaskGroup(group_id="tg-healthgrade_dataflow_job") as tablegroup:
                run_dataflow_job = [
                    BashOperator(
                        task_id=f"run_healthgrade_dataflow_job",
                        dag=dag,
                        bash_command="sleep $(shuf -i 1-300 -n 1); python /home/airflow/gcs/dags/{lob}/sql/",
                        # Note: Your original code seems incomplete here - you may need to fix this line
                        # srcsys_config["v_filebqtemplate"], src_config_file, env_config_file
                    ),
                ]

        wait_for_python_job_async_done = DataflowJobStatusSensor(
            task_id="wait_for_python_job_async_done",
            job_id="{{ task_instance.xcom_pull(task_ids='run_dataflow_jobs.tg-healthgrade_dataflow_job.run_healthgrade_dataflow_job', key='return_value') }}",
            expected_statuses=(DataflowJobStatus.JOB_STATE_DONE,),
            mode="reschedule",
            poke_interval=360,
            location="us-east4",
        )

        run_dataflow_job >> wait_for_python_job_async_done

        # Updated task dependencies
        wait_for_sftp_job_done >> tg1 >> end_job

    return dag

# Keep your existing DAG generation logic
lob = srcsys_config["lob"]
source_system = srcsys_config["sourcesysnm"]
for schedule in srcsys_config["schedule"]:
    frequency = schedule["frequency"]
    schedule_interval = schedule["schedule_interval"]
    start_date = schedule["start_date"]

    if schedule_interval == "none":
        interval_range = "None"
        schedule = None
    else:
        schedule = schedule_interval
        time = schedule.split(" ")
        interval_range = time[1].zfill(2) + time[0].zfill(2)

    dag_id = f"dag_ingest_healthgrade_" + frequency + "_" + interval_range
    globals()[dag_id] = create_dag(
        dag_id,
        schedule,
        start_date,
        source_system,
        frequency
    )
