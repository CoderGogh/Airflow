import pendulum
from airflow.sdk import DAG, task

with DAG(
    dag_id="dag_python_with_xcom_ex1",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task(task_id='python_xcom_push_task1')
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_1')
        ti.xcom_push(key='result2', value=[1, 2, 3])


    @task(task_id='python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_2')
        ti.xcom_push(key='result2', value=[1, 2, 3, 4])

    @task(task_id='python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        # Airflow 3: task_ids 생략 시 자기 자신에서 찾으므로 명시 필요
        value1 = ti.xcom_pull(task_ids='python_xcom_push_task2', key='result1')
        value2 = ti.xcom_pull(task_ids='python_xcom_push_task1', key='result2')
        print(value1)
        print(value2)

    xcom_push1() >> xcom_push2() >> xcom_pull()
