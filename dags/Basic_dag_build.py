# Import inbuit module 
from datetime import datetime, timedelta

# Import Airflow module
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator



def print_world():
    print('Welcome to Airflow tutorial at Pydata London 2018')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    # You want an owner and possibly a team alias
    'email': ['yourteam@example.com', 'you@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('airflow_tutorial_v01',
         default_args=default_args,
         # schedule_interval='0 * * * *',
         ) as dag:

    print_hello = BashOperator(
                    task_id='print_hello',
                    bash_command='echo "hello"'
                    )

    sleep = BashOperator(
                    task_id='sleep',
                    bash_command='sleep 5'
                    )

    print_world = PythonOperator(
                    task_id='print_world',
                    python_callable=print_world
                    )

# one way of setting dependencies
print_hello >> sleep >> print_world

# other way of setting dependencies
# sleep.set_upstream(print_hello) # Equivalent to print_hello.set_downstream(sleep)
# print_world.set_upstream(sleep) # Equivalent to sleep.set_downstream(print_world)
 