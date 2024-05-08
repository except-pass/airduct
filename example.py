import pendulum
from airflow.decorators import dag, task

from airduct import distribute_iterable, make_parallel_processes

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={'provide_context': True},
)
def airduct_example():

    def long_running_task(item, *args, **kwargs):
        '''
        Simulate a long running task
        '''
        import time
        print(f'Processing {item}')
        time.sleep(5)
        return item

    @task()
    def generate_numbers():
        numbers = list(range(1, 10))
        return numbers

    @task()
    def distribute_numbers(numbers, parallel):
        return distribute_iterable(numbers, parallel=parallel)

    parallel = 3

    numbers = generate_numbers()
    #implicitly this does numbers >> distributed
    distributed = distribute_numbers(numbers, parallel=parallel)
    

    paralleled_processes = make_parallel_processes(task_id_stub='process_parallel', 
                                      func=long_running_task, 
                                      from_task_id='distribute_numbers', 
                                      parallel=parallel)
    #even though assigning numbers above implicitly links `numbers` and `distributed`, I find it more clear to spell it out here
    numbers >> distributed >> paralleled_processes

airduct_example()