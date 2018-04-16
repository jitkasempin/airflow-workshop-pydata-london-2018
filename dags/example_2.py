from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta
import sys
import time

dagid   = 'DA' + str(int(time.time()))
taskid  = 'TA' + str(int(time.time()))

input_file = '/home/directory/airflow/textfile_for_dagids_and_schedule'

def my_sleeping_function(random_base):
    '''This is a function that will run within the DAG execution'''
    time.sleep(random_base)

def_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(), 'email_on_failure': False,                
    'retries': 1, 'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id=dagid, 
    default_args=def_args,
    )

with open(input_file,'r') as f:
    for line in f:
        args = line.strip().split('\t')
    if len(args) < 2:
        continue
    taskid = args[0]
    argument = args[1]
    type_arg = args[2]

    t1 = DummyOperator(
    task_id='extract_data',
    dag=dag

    if type_arg == 'sleep':
        myBashTask = BashOperator(
            task_id=taskid,
            bash_command='sleep {{ sleep }}',
            params = {'sleep' : int(argument)}
            dag=dag)
    else:
        myBashTask = BashOperator(
            task_id=taskid,
            bash_command='echo {{ prt }}',
            params = {'prt' : str(argument)}
            dag=dag)

    t1 >> myBashTask

f.close()