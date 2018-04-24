# import in-built module
import os
import csv

# import airflow module and operator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG
from datetime import datetime, timedelta

# Absolute path for config file
input_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'example_cfg.csv')

dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(), 'email_on_failure': False,
    'retries': 1, 'retry_delay': timedelta(minutes=2)
}

dag = DAG(
    dag_id='DAG_2_Dynamic_dag_example',
    default_args=dag_args,
)

i = 0  # Counter for dummy variable

with open(input_file, 'r') as f:
    reader = csv.reader(f)
    for line in reader:
        taskid = line[0]
        argument = line[1]
        type_arg = line[2]
        t1 = DummyOperator(
            task_id='dummy_dag_{}'.format(i),
            dag=dag
        )
        i = i + 1
        t2 = DummyOperator(
            task_id='dummy_dag_{}'.format(i),
            dag=dag
        )

        if type_arg == 'slp':
            myBashTask = BashOperator(
                task_id='task_sleep_{}'.format(i),
                bash_command='sleep {{ sleep }}',
                params={'sleep': int(argument)},
                dag=dag)
        elif type_arg == 'ech':
            myBashTask = BashOperator(
                task_id='task_echo_{}'.format(i),
                bash_command='echo {{ prt }}',
                params={'prt': str(argument)},
                dag=dag)

        t1 >> myBashTask >> t2
        # myBashTask.set_upstream(t1)
        # t2.set_upstream(myBashTask)

f.close()
