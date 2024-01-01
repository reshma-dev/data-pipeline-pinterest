from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

import base64
import codecs

path = f"{codecs.decode('/Ercbf/erfuzn', 'rot_13')}{int(base64.b64decode('OTEy'.encode('utf-8')).decode('utf-8'))}\
{codecs.decode('@lnubb.pbz/qngn-cvcryvar-cvagrerfg/qngnoevpxf-abgrobbxf/ongpu_cebprffvat', 'rot_13')}"

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': f'{path}',
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': 'reshma',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=30)
}


with DAG('12e371d757c1_dag',
    # should be a datetime format
    start_date=datetime(2024, 1, 1),
    # check out possible intervals, should be a string
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
