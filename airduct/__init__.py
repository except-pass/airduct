import logging
from typing import Iterable, List, Dict

from airflow.decorators import task
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

    #expecting a form of {parallel_num: {item:result, item2:result2, ...}}
    iterate_over = ti.xcom_pull(task_ids=from_task_id)
    log.info(f"Got {iterate_over} from {from_task_id}")
    if iterate_over is None:
        raise ValueError(f"Nothing to iterate over from {from_task_id}.  Are you sure it exists and returns a value?")
    #airflow coerses keys into strings
    return iterate_over[str(parallel)]

def batch_process(func, from_task_id, parallel, func_args=None, func_kwargs=None, include_airflow_context=False, **kwargs):
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
    if include_airflow_context:
        func_kwargs['airflow_context'] = kwargs
    iterate_over = iterate_over_these(from_task_id=from_task_id, parallel=parallel, kwargs=kwargs)
    results = {}
    for item, prevresult in iterate_over.items():  #this doesn't handle dicts very well.  Need to rethink all this
        result = func(item, prevresult, *func_args, **func_kwargs)
        results[item] = result
    return {parallel: results}

@task
def distribute_iterable(master_iterable:Iterable, parallel:int, **kwargs):
    print(f"Distributing iterable: {master_iterable}")
    split_lists = [master_iterable[i::parallel] for i in range(parallel)]
    print(f'Split into {parallel} lists: {split_lists}')
    returnme = {}
    for list_num, lst in enumerate(split_lists):
        returnme[list_num] = {item:None for item in lst}
        
    log.info(f"Split to {parallel} lists: {returnme}")
    return returnme

@task
def read_iterable(**airflow_context):
    #check the cli for overrides, otherwise get some default behavior from function arguments
    instructions = airflow_context['params'].get('iterate_over', {})

    method = instructions.get('method', 'cli')
    keyword = instructions.get('keyword')

    iters = None
    if method == 'cli':  #keyword is the param key
        iters = instructions.get(keyword)
    
    if method == 'file': #keyword is the file path
        with open(instructions.get(keyword)) as f:
            iters = [line.strip() for line in f.readlines()]
    if iters is None:
        raise ValueError("No iterable found")
    return iters   

def make_parallel_processes(task_id_stub, func, from_task_id, parallel, func_args=None, func_kwargs=None, include_airflow_context=False):
    '''
    from_task_id accepts templates.  Use {parallel_num} to insert the parallel number
    '''
    batch_processes = []
    for parallel_num in range(parallel):
        batch_processes.append(
            PythonOperator(
                task_id=f'{task_id_stub}_{parallel_num}',
                python_callable=batch_process,
                op_args=[func, from_task_id.format(parallel_num=parallel_num), parallel_num],
                op_kwargs={'func_args':func_args, 'func_kwargs':func_kwargs, 'include_airflow_context':include_airflow_context},
                provide_context=True,
            )
        )
    return batch_processes