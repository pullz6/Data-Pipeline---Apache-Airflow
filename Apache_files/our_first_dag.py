from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'Sara',
    'retries' : 5, 
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'our_first_dag_v5',
    default_args = default_args,
    description = 'This is our first day that we write',
    start_date = datetime(2021,7,29,2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'First_Task',
        bash_command = 'echo Hello World, this is the first task!'
    )
    
    task2 = BashOperator(
        task_id = 'Second_Task',
        bash_command = 'echo hey, I am task 2 and will be running after task 1'
    )
    
    task3 = BashOperator(
        task_id = 'Third_Task',
        bash_command = 'echo hey, I am task 3 and will be running after task 1 at the same time as task 2'
    )
    
    #Method 1 
    #task1.set_downstream(task2)
    #task1.set_downstream(task3)
    
    #Method 2
    #task1 >> task2
    #task1 >> task3
    
    #Method 3
    task1 >> [task2,task3]
    
    
    