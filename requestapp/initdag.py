#script by rikardoroa
#just python it!
from requestapp.script import geographic_data
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator


default_args = {
  'start_date': datetime.today().strftime('%Y-%m-%d')
}

geo_data = geographic_data()

with DAG ('request_data', schedule_interval='@hourly',dagrun_timeout=timedelta(minutes=10),max_active_runs=1, default_args = default_args , catchup=False) as dag:
  
  response_data = PythonOperator(
  task_id='response_data',
  python_callable=geo_data.response,
  execution_timeout=timedelta(seconds=20)
  )
  
  writing_directories = PythonOperator(
  task_id='writing_directories',
  python_callable=geo_data.write_dirs,
  execution_timeout=timedelta(seconds=20)
  )

  writing_data_files = PythonOperator(
  task_id='writing_data_files',
  python_callable=geo_data.write_files,
  execution_timeout=timedelta(seconds=20)
  )

  data_processing = PythonOperator(
  task_id='data_processing',
  python_callable=geo_data.processing_data,
  execution_timeout=timedelta(seconds=20)
  )

  mergin_data = PythonOperator(
  task_id='mergin_data',
  python_callable=geo_data.merging_df_data,
  execution_timeout=timedelta(seconds=20)
  )

  join_some_data = PythonOperator(
  task_id='join_some_data',
  python_callable=geo_data.join_data,
  execution_timeout=timedelta(minutes=10)
  )
  
  response_data >> writing_directories >> writing_data_files >> data_processing >> mergin_data >> join_some_data
 
        