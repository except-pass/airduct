import logging
from typing import Iterable, List

from airflow.operators.python import PythonOperator
from airflow.models import taskinstance

log = logging.getLogger(__name__)

def get_task_instance(kwargs)->taskinstance:
    return kwargs['ti']

def iterate_over_these(from_task_id, parallel, kwargs):
    '''
    kwargs come from airflow
    '''
    ti = get_task_instance(kwargs)
    iterate_over = ti.xcom_pull(task_ids=from_task_id)
    return iterate_over[parallel]

def batch_process(func, from_task_id, parallel, func_args=None, func_kwargs=None, **kwargs):
    '''
    funcargs and funckwargs are the arguments and keyword arguments that will be passed to func
    kwargs come from airflow

    Assumes that the previous task has pushed a list of items to be processed to xcom.  
        e.g. You want 3 parallel tasks you would get the split of 
    [ [1, 4, 7], [2, 5, 8], [3, 6, 9] ]

    func must accept the item to be processed as the first argument 
        e.g.: process_serial_number(serial_number, *args, **kwargs)
    '''
    func_args = func_args or []
    func_kwargs = func_kwargs or {}
    iterate_over = iterate_over_these(from_task_id=from_task_id, parallel=parallel, kwargs=kwargs)
    results = {}
    for item in iterate_over:
        result = func(item, *func_args, **func_kwargs)
        results[item] = result
    return results

def distribute_iterable(master_iterable:Iterable, parallel:int)->List[List]:
    split_lists = [master_iterable[i::parallel] for i in range(parallel)]
    log.info(f"Got numbers: {split_lists}")        
    return split_lists

def make_parallel_processes(task_id_stub, func, from_task_id, parallel, func_args=None, func_kwargs=None):
    batch_processes = []
    for i in range(parallel):
        batch_processes.append(
            PythonOperator(
                task_id=f'{task_id_stub}_{i}',
                python_callable=func,
                op_args=[f'distribute_numbers', i],
                op_kwargs={'func_args':func_args, 'func_kwargs':func_kwargs},
                provide_context=True,
            )
        )
    return batch_processes

