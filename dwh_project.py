from datetime import timedelta, datetime
import os
from collections import namedtuple

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.models import TaskInstance

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
        name='START', latest_only = False,
        list_jobs=(
            #FactoryJob(name='INIT_VAR', source_path='var', mask='.sql'),
            FactoryJob(name='LOAD_ODS_VIEWS', source_path='views', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='ETL', latest_only = False,
        list_jobs=(
            FactoryJob(name='HUBS', source_path='hubs', mask='.sql'),
            FactoryJob(name='LINKS', source_path='links', mask='.sql'),
            FactoryJob(name='SATELLITES', source_path='satellites', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='DM', latest_only = True,
        list_jobs=(
            FactoryJob(name='CREATE_TMP_REPORT', source_path='tmp_report', mask='.sql'),
            FactoryJob(name='DIMENSIONS', source_path='dimensions', mask='.sql'),
            FactoryJob(name='FACTS', source_path='facts', mask='.sql'),
        )
    ),
    FactoryPhase(
        name='FINISH', latest_only = False,
        list_jobs=(
            FactoryJob(name='CLEAR', source_path='clear', mask='.sql'),
        )
    ),
)

'''
def check_status(**kwargs):
    execute_date = kwargs['execute_date']
    end_date = kwargs['end_date']
    #ti = TaskInstance(, end_date)
    #state = ti.current_state()
    #if state != 'success':
    for task_instance in kwargs['execute_date'].get_task_instance():
        if task_instance.current_state() != State.SUCCESS and task_instance.task_id != kwargs['task_instance'].task_id:
            return False
    return True
'''        

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

'''
def get_check_phase(phase):
    return ShortCircuitOperator(
        task_id=,
        provide_context=True,
        python_callable=check_status,
        dag=dag,
    )
'''

def get_check_point(phase_name, job_name):
    return DummyOperator(task_id="{}_{}_complete".format(phase_name, job_name), dag=dag)


default_args = {
    'owner': USERNAME,
    'depends_on_past': True,
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
    #concurrency=1,
    max_active_runs=1,
)


check_point_last = None
for phase in PHASES:   
    for job in phase.list_jobs:
        check_point = get_check_point(phase.name, job.name)
        if check_point_last:
            check_point_last >> get_job_context(phase.name, job) >> check_point
        else:
            get_job_context(phase.name, job) >> check_point
        check_point_last = check_point
        

