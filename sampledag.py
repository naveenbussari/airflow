import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Give the DAG name, configure the schedule, and set the DAG settings

dag_spark = DAG(
    dag_id = "sample_dag",
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',	
    dagrun_timeout=timedelta(minutes=60),
    description='use case of sparkoperator in airflow',
    start_date = airflow.utils.dates.days_ago(1)
)

#setting up the tasks which want all the tasks in the workflow

spark_submit_local = BashOperator(

    task_id='sample_task', 
    dag=dag_spark,
    bash_command='echo HELLO'
)

spark_submit_local
