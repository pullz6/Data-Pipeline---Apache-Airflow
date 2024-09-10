from airflow.decorators import dag, task 

from datetime import datetime, timedelta

default_args = {
    'owner' : 'Sara',
    'retries' : 5, 
    'retry_delay': timedelta(minutes=2)
}

@dag(default_args=default_args,
     dag_id='dag_taskflow_api_v01',
     start_date=datetime(2024, 9, 9),
     schedule_interval='@daily'
     )

def hello_world_etl(): 
    
    @task()
    def get_name():
        return 'Jerry'
    
    @task()
    def get_age(): 
        return '25'
    
    @task()
    def greet(name, age): 
        print(f'My name is {name} and my age is {age}')
        
    name =get_name()
    age = get_age()
    greet(name=name, age=age)
    
greet_dag = hello_world_etl()