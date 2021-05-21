from datetime import timedelta, datetime
import os
from collections import namedtuple

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator


USERNAME = 'yfurman'
HOME_PATH = 'furman2'

ROOT_DIR = os.getenv('AIRFLOW__CORE__DAGS_FOLDER', '/root/airflow/dags')
DATA_DIR = '{}/SQL'.format(HOME_PATH)

DATABASE_NAME = USERNAME
PREFIX_NAME = '{}.project'.format(DATABASE_NAME)

FactoryPhase = namedtuple('FactoryPhase', ['name', 'latest_only', 'list_jobs'])
FactoryJob = namedtuple('FactoryJob', ['name', 'source_path', 'mask'])

PHASES = (
    FactoryPhase(
        name='ETL', latest_only = False,
        list_jobs=(
            FactoryJob(name='ODS', source_path='ods', mask='.sql'), 
            FactoryJob(name='LOAD_VIEWS', source_path='views', mask='.sql'),
            FactoryJob(name='HUBS', source_path='hubs', mask='.sql'),
            FactoryJob(name='LINKS', source_path='links', mask='.sql'),
            FactoryJob(name='SATELLITES', source_path='satellites', mask='.sql'),
            FactoryJob(name='CLEAR_VIEWS', source_path='clear_views', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='DM', latest_only = True,
        list_jobs=(
            FactoryJob(name='CREATE_TMP_REPORT', source_path='tmp_report', mask='.sql'),
            FactoryJob(name='DIMENSIONS', source_path='dimensions', mask='.sql'),
            FactoryJob(name='FACTS', source_path='facts', mask='.sql'),
            FactoryJob(name='CLEAR_TMP', source_path='clear_tmp', mask='.sql'),
        )
    ),
)

def get_job_context(phase_name, job):
    tasks = []
    for task_file_name in [i for i in os.listdir(os.path.join(ROOT_DIR, DATA_DIR, job.source_path)) if i.endswith(job.mask)]:
        tasks.append(PostgresOperator(
            task_id='{}_{}_{}'.format(phase_name, job.name, os.path.splitext(task_file_name)[0]),
            dag=dag,
            params={'prefix': PREFIX_NAME},
            sql=os.path.join(job.source_path, task_file_name)
        ))            
    return tasks

default_args = {
    'owner': USERNAME,
    #'depends_on_past': True,
    'depends_on_past': False,
    'start_date': datetime(2010, 1, 1, 0, 0, 0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=120),
}

dag = DAG(
    PREFIX_NAME,
    default_args=default_args,
    description='Data Warehouse - Project Work', 
    template_searchpath=[os.path.join(ROOT_DIR, DATA_DIR)],         
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)

check_point_last = None
for phase in PHASES:  
    if phase.latest_only:
        last_only_point = LatestOnlyOperator(task_id="{}_latest_only".format(phase.name), dag=dag)
        if check_point_last:
            check_point_last >> last_only_point
            check_point_last = last_only_point
    for job in phase.list_jobs:
        check_point = DummyOperator(task_id="{}_{}_complete".format(phase.name, job.name), dag=dag)
        job_context = get_job_context(phase.name, job)
        if check_point_last:
            check_point_last >> job_context >> check_point          
        else:
            job_context >> check_point
        check_point_last = check_point
        

