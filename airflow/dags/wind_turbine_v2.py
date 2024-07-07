from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import os

default_args = {
    'depends_on_past': False,
    'email': ['monteiro.tec32@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('windturbine_v2', description='Dados da Turbina', schedule_interval="*/3 * * * *", start_date=datetime(2024, 3, 1), catchup=False, default_args=default_args, default_view='graph', doc_md='## Dag para registrar dados de turbina eólica')

group_check_temp = TaskGroup("group_check_temp", dag=dag)
group_database = TaskGroup("group_database", dag=dag)

file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    filepath=Variable.get('path_file'),
    fs_conn_id='fs_default',
    poke_interval=10,
    dag=dag
)

def process_file(**kwargs):
    try:
        file_path = Variable.get('path_file')
        with open(file_path) as f:
            data = json.load(f)
            for record in data:
                kwargs['ti'].xcom_push(key=f'turbine_id_{record["turbine_id"]}', value=record['turbine_id'])
                kwargs['ti'].xcom_push(key=f'powerfactor_{record["turbine_id"]}', value=record['powerfactor'])
                kwargs['ti'].xcom_push(key=f'hydraulicpressure_{record["turbine_id"]}', value=record['hydraulicpressure'])
                kwargs['ti'].xcom_push(key=f'temperature_{record["turbine_id"]}', value=record['temperature'])
                kwargs['ti'].xcom_push(key=f'timestamp_{record["turbine_id"]}', value=record['timestamp'])
        os.remove(file_path)
    except Exception as e:
        print(f"Error processing file: {e}")
        raise

get_data = PythonOperator(
    task_id='get_data',
    python_callable=process_file,
    provide_context=True,
    dag=dag
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres',
    sql=''' 
    CREATE TABLE IF NOT EXISTS SENSORS(
        IDTEMP SERIAL PRIMARY KEY,  
        TURBINE_ID VARCHAR, 
        POWERFACTOR VARCHAR, 
        HYDRAULICPRESSURE VARCHAR, 
        TEMPERATURE VARCHAR, 
        TIMESTAMP VARCHAR
    );
    ''',
    task_group=group_database,
    dag=dag
)

def generate_insert_statements(ti, **kwargs):
    insert_statements = []
    for turbine_id in range(1, 11):  
        turbine_id_value = ti.xcom_pull(task_ids='get_data', key=f'turbine_id_{turbine_id}')
        powerfactor_value = ti.xcom_pull(task_ids='get_data', key=f'powerfactor_{turbine_id}')
        hydraulicpressure_value = ti.xcom_pull(task_ids='get_data', key=f'hydraulicpressure_{turbine_id}')
        temperature_value = ti.xcom_pull(task_ids='get_data', key=f'temperature_{turbine_id}')
        timestamp_value = ti.xcom_pull(task_ids='get_data', key=f'timestamp_{turbine_id}')
        
        if turbine_id_value:  # Verifica se os dados foram puxados corretamente
            insert_statement = f'''
                INSERT INTO SENSORS (TURBINE_ID, POWERFACTOR, HYDRAULICPRESSURE, TEMPERATURE, TIMESTAMP)
                VALUES ('{turbine_id_value}', '{powerfactor_value}', '{hydraulicpressure_value}', '{temperature_value}', '{timestamp_value}');
            '''
            insert_statements.append(insert_statement)
    
    final_sql = " ".join(insert_statements)
    ti.xcom_push(key='insert_sql', value=final_sql)

generate_sql_task = PythonOperator(
    task_id='generate_sql',
    python_callable=generate_insert_statements,
    provide_context=True,
    dag=dag
)

insert_data = PostgresOperator(
    task_id='insert_data',
    postgres_conn_id='postgres',
    sql="{{ ti.xcom_pull(task_ids='generate_sql', key='insert_sql') }}",
    task_group=group_database,
    dag=dag
)

exec_proc = PostgresOperator(
    task_id='exec_procedure',
    postgres_conn_id='postgres',
    sql='''CALL alerts_generate();''',
		task_group=group_database,
    dag=dag
)

def avalia_temp(**context):
    temperatures = [float(context['ti'].xcom_pull(task_ids='get_data', key=f'temperature_{turbine_id}')) for turbine_id in range(1, 11)]
    high_temp_ids = [str(turbine_id) for turbine_id, temp in enumerate(temperatures, 1) if temp >= 24]
    context['ti'].xcom_push(key='high_temp_ids', value=', '.join(high_temp_ids))
    # high_temp_ids = [i + 1 for i, temp in enumerate(temperatures) if temp >= 24]
    # context['ti'].xcom_push(key='high_temp_ids', value=high_temp_ids)

    if high_temp_ids:
        return 'group_check_temp.send_email_alert'
    else:
        return 'group_check_temp.send_email_normal'

send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='monteiro.tec32@gmail.com',
    subject='Airflow Alert',
    html_content='''<h3>Turbinas com temperatura acima de 24°C: {{ task_instance.xcom_pull(task_ids='group_check_temp.check_temp_branch', key='high_temp_ids') }}</h3>''',
    task_group=group_check_temp,
    dag=dag
)

send_email_normal = EmailOperator(
    task_id='send_email_normal',
    to='monteiro.tec32@gmail.com',
    subject='Airflow Advise',
    html_content='''<h3>Temperaturas Normais</h3><p> Dag: windturbine</p>''',
    task_group=group_check_temp,
    dag=dag
)



check_temp_branch = BranchPythonOperator(
    task_id='check_temp_branch',
    python_callable=avalia_temp,
    provide_context=True,
    dag=dag,
    task_group=group_check_temp
)

with group_check_temp:
    check_temp_branch >> [send_email_alert, send_email_normal]

with group_database:
    create_table >> generate_sql_task >> insert_data >> exec_proc

file_sensor_task >> get_data
get_data >> group_check_temp
get_data >> group_database
