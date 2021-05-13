from datetime import timedelta, datetime
import os
from collections import namedtuple

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'yfurman'
HOME_PATH = 'furman2'

ROOT_DIR = os.getenv('AIRFLOW__CORE__DAGS_FOLDER', '/root/airflow/dags')
#DATA_DIR = '{}/SQL'.format(USERNAME)
DATA_DIR = '{}/SQL'.format(HOME_PATH)

DATABASE_NAME = USERNAME
PREFIX_NAME = '{}.project'.format(DATABASE_NAME)


'''
INIT_PHASE = ('VAR', 'LOAD_ODS_VIEWS',)
ETL_PHASE = ('HUBS', 'LINKS', 'SATELLITES', )
DM_PHASE = ('CREATE_TMP_REPORT', 'DIMENSIONS', 'FACTS',)
#FINISH_PHASE = ('DROP_ODS_VIEWS', 'DROP_TMP_REPORT', 'DROP_VAR',)
FINISH_PHASE = ('CLEAR',)
'''

FactoryPhase = namedtuple('FactoryPhase', ['name', 'list_jobs'])
FactoryJob = namedtuple('FactoryJob', ['name', 'source_path', 'mask'])

PHASES = (
    FactoryPhase(
        name='START',
        list_jobs=(
            #FactoryJob(name='INIT_VAR', source_path='var', mask='.sql'),
            FactoryJob(name='LOAD_ODS_VIEWS', source_path='views', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='ETL',
        list_jobs=(
            FactoryJob(name='HUBS', source_path='hubs', mask='.sql'),
            FactoryJob(name='LINKS', source_path='links', mask='.sql'),
            FactoryJob(name='SATELLITES', source_path='satellites', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='DM',
        list_jobs=(
            FactoryJob(name='CREATE_TMP_REPORT', source_path='tmp_report', mask='.sql'),
            FactoryJob(name='DIMENSIONS', source_path='dimensions', mask='.sql'),
            FactoryJob(name='FACTS', source_path='facts', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='FINISH',
        list_jobs=(
            FactoryJob(name='CLEAR', source_path='clear', mask='.sql'),
        )
    ),
)

'''
def get_jobs_context(jobs_name):
    tasks = []
    list_task_files = [i for i in os.listdir(os.path.join(ROOT_DIR, DATA_DIR, jobs_name)) if i.endswith('.sql')]
    for task_file_name in list_task_files:
        with open(os.path.join(ROOT_DIR, DATA_DIR, jobs_name, task_file_name)) as task_file:
            tasks.append(PostgresOperator(
                task_id='{}_{}'.format(jobs_name, os.path.splitext(task_file_name)[0])),
                dag=dag,
                sql=task_file.read()
            ))            
    return tasks
'''

'''
def get_jobs_context(phase_name, job):
    tasks = []
    list_task_files = [i for i in os.listdir(os.path.join(ROOT_DIR, DATA_DIR, job.source_path)) if i.endswith(job.mask)]
    for task_file_name in list_task_files:
        with open(os.path.join(ROOT_DIR, DATA_DIR, job.source_path, task_file_name)) as task_file:
            tasks.append(PostgresOperator(
                task_id='{}_{}_{}'.format(phase_name, job.name, os.path.splitext(task_file_name)[0])),
                dag=dag,
                sql=task_file.read()
            ))            
    return tasks
'''


def get_jobs_context(phase_name, job):
    tasks = []
    search_path = os.path.join(ROOT_DIR, DATA_DIR, job.source_path)
    for task_file_name in [i for i in os.listdir(search_path) if i.endswith(job.mask)]:
        tasks.append(PostgresOperator(
            task_id='{}_{}_{}'.format(phase_name, job.name, os.path.splitext(task_file_name)[0]),
            dag=dag,
            template_searchpath=[search_path],         
            sql=task_file_name
        ))            
    return tasks


def get_check_point(phase_name, job_name):
    return DummyOperator(task_id="{}_{}_complete".format(phase_name, job_name), dag=dag)


default_args = {
    'owner': USERNAME,
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
    schedule_interval="0 0 1 1 *",
    concurrency=1,
    max_active_runs=1,
)

'''
for phase in (INIT_PHASE, ETL_PHASE, DM_PHASE, FINISH_PHASE):
    check_point = None
    for jobs in phase:
        check_point = get_check_point(jobs)
        get_jobs_context(jobs) >> check_point
'''

for phase in PHASES:
    check_point = None
    for job in phase.list_jobs:
        check_point = get_check_point(phase.name, job.name)
        get_jobs_context(phase.name, job) >> check_point
        

