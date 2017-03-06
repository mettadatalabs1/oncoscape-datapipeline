from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import  DummyOperator
from datetime import datetime

# Provider Directory imports
from mies.common.process_data_source import process_data_source_helper as psd


default_args = {
        "owner": "CDW",
        "depends_on_past": False,
        "start_date": datetime(2017, 2, 28),
        "schedule_interval": "0 * * * *",
}
"""        
pd_raw_staging_dag (airflow.DAG)
The main dag that controls the flow for PD load
"""
pd_raw_staging_dag = DAG("ProviderDirectoryRawStagingDAG", default_args=default_args)
#TODO (kgomadam, mputcha): These operators become a Python Operator in the
# actual executor with the exception of the monitor_sftp_folder. This becomes a
# SFTPTriggerOperator.Airflow does not yet have that. We need to implement it. 
#monitor_sftp_folder (airflow.operators.dummy_operator.DummyOperator)   
monitor_sftp_folder = DummyOperator(task_id="monitor_sftp_folder",
                                     dag=pd_raw_staging_dag)
#raw_loader(airflow.operators.dummy_operator.DummyOperator)   
raw_loader = PythonOperator(task_id="raw_loader",
                            python_callable=psd.get_provider_directory,
                            op_args=["NPI",],
                            dag=pd_raw_staging_dag)
#raw_exception(airflow.operators.dummy_operator.DummyOperator)   
# Set up the process_feed portion of the workflow
raw_exception = DummyOperator(task_id="raw_exception",dag=pd_raw_staging_dag)
monitor_sftp_folder.set_downstream(raw_loader)
raw_loader.set_downstream(raw_exception)
# TODO: SubDAG these

