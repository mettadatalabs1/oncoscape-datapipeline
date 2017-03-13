from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import  DummyOperator
from datetime import datetime



default_args = {
        "owner": "fred_hutch_sttr",
        "depends_on_past": False,
        "start_date": datetime(2017, 3, 12),
        "schedule_interval": "* * * * *",
}
"""        
DAG for clinical and molecular data pipeline.
Main tasks in this DAG are:
    1. Parse the job configuration json and return the job parameters
    2. Load the job file
    3. Validate the job file based on the file data type
    4. Validate the gene symbol and recommend changes, if needed
    5. Data transformations for clinical data
    6. Insert the processed, transformed data into MongoDB
    7. Log after each task and update the job status
"""
# Declare the DAG artifacts
#oncoscape_main_dag (airflow.DAG)   
oncoscape_main_dag = DAG("OncoscapeMainDAG", default_args=default_args)
#parse_job_config (airflow.operators.dummy_operator.DummyOperator)   
parse_job_config = DummyOperator(task_id="parse_job_config",
                                     dag=oncoscape_main_dag)
#job_file_datatype_validate (airflow.operators.dummy_operator.DummyOperator)   
job_file_datatype_validate = PythonOperator(task_id="data_type_validator",
                            python_callable=None,
                            op_args=["dummy",],
                            dag=oncoscape_main_dag)

# create the DAG by setting the execution order for the operators
parse_job_config.set_downstream(job_file_datatype_validate)

