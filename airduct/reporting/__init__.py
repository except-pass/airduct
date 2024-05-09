import os
from pathlib import Path

from retrying import retry

AIRFLOW_HOME = Path(os.environ.get('AIRFLOW_HOME', '~/airflow'))

def get_ids(airflow_context):
    #kwargs are airflow context    
    dag_id = airflow_context['dag'].dag_id
    run_id = airflow_context['run_id']
    task_id = airflow_context['task'].task_id
    execution_date = airflow_context['ds']    
    return {'dag_id': dag_id, 'run_id': run_id, 'task_id': task_id, 'execution_date': execution_date}

def get_log_path(dag_id=None, run_id=None, airflow_context=None)->Path:
    #kwargs are airflow context
    if None in (dag_id, run_id):
        ids = get_ids(airflow_context)
        dag_id = ids['dag_id']
        run_id = ids['run_id']

    log_path = AIRFLOW_HOME/'logs'/f'dag_id={dag_id}'/f'run_id={run_id}'
    return log_path

def get_task_path(dag_id=None, run_id=None, task_id=None, airflow_context=None)->Path:
    if None in (dag_id, run_id, task_id):
        ids = get_ids(airflow_context)
        dag_id = ids['dag_id']
        run_id = ids['run_id']
        task_id = ids['task_id']

    log_path = get_log_path(dag_id=dag_id, run_id=run_id, airflow_context=airflow_context)
    task_path = log_path/f'task_id={task_id}'
    return task_path

#retry with exponential backoff to prevent parallel tasks from stepping on each other
@retry(wait_exponential_multiplier=10, wait_exponential_max=1000, stop_max_attempt_number=5)
def write_to_file(fpath, content, access_mode='a'):
    with open(fpath, access_mode) as f:
        f.write(content)

def write_to_run_log(fname, content, airflow_context, access_mode='a'):
    log_path = get_log_path(airflow_context=airflow_context)
    write_to_file(log_path/fname, content, access_mode=access_mode)

def write_to_task_log(fname, content, airflow_context, access_mode='a'):
    task_path = get_task_path(airflow_context=airflow_context)
    write_to_file(task_path/fname, content, access_mode=access_mode)
