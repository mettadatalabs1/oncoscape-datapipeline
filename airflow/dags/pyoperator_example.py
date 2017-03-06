from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
        "owner": "CDW",
        "depends_on_past": False,
        "start_date": datetime(2015, 6, 1),
        "schedule_interval": "0 * * * *",
        }

example_dag = DAG("PythonExampleDAG", default_args=default_args)

def ex_func_add(a,b,c):
    print c
    return a + b

ex_task1 = PythonOperator(task_id="add1",python_callable=ex_func_add, op_args=[2,3,"task1",],dag=example_dag)
ex_task2 = PythonOperator(task_id="add2",python_callable=ex_func_add, op_args=[2,3,"task2",],dag=example_dag)
ex_task3 = PythonOperator(task_id="add3",python_callable=ex_func_add, op_args=[2,3,"task2",],dag=example_dag)
ex_task1.set_downstream(ex_task2)
ex_task1.set_downstream(ex_task3)
