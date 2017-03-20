from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import  DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import DagRun
from airflow import settings
from datetime import datetime


from workflow.executors.validate_file import validate_file, validate_hugo




def trigger_oncoscape_pipeline(context, dag_run_obj):
    print ("*"*10)
    print (dag_run_obj)
    print ("*"*10)
    c_p =context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    return dag_run_obj

default_args = {
        "owner": "fred_hutch_sttr",
        "depends_on_past": False,
        "start_date": datetime.now()
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


oncoscape_main_dag = DAG(dag_id="oncoscape_DAG",
                         default_args=default_args,
                         schedule_interval="@once")
#parse_job_config (airflow.operators.dummy_operator.DummyOperator)
pipeline_trigger_dag = DAG(dag_id='oncoscape_trigger_DAG',
                           default_args={"owner": "fred_hutch_sttr",
                                         "start_date": datetime.now()},
                           schedule_interval='@once')

trigger = TriggerDagRunOperator(task_id='trigger_oncoscape_pipeline',
                                trigger_dag_id="oncoscape_DAG",
                                python_callable=trigger_oncoscape_pipeline,
                                params={'condition_param': True,
                                        'message': 'Hello World'},
                                dag=pipeline_trigger_dag)

session = settings.Session()
current_run = session.query(DagRun).filter(
    DagRun.dag_id.in_(["oncoscape_trigger_DAG"])
    ).all()
print (current_run)
print ("@"*10)
parse_job_config = DummyOperator(task_id="parse_job_config",
                                     dag=oncoscape_main_dag)
#job_file_datatype_validate (airflow.operators.dummy_operator.DummyOperator)
job_file_datatype_validate = PythonOperator(task_id="data_type_validator",
                            python_callable=validate_file,
                            op_args=["dummy",],
                            dag=oncoscape_main_dag)
#job_file_datatype_validate (airflow.operators.dummy_operator.DummyOperator)
hugo_validate = PythonOperator(task_id="hugo_validator",
                            python_callable=validate_hugo,
                            op_args=["dummy",],
                            dag=oncoscape_main_dag)

# create the DAG by setting the execution order for the operators
parse_job_config.set_downstream(job_file_datatype_validate)


def main(args):
    pass

if __name__ == "__main__":
    main(None)
