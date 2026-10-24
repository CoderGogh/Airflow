from airflow.sdk import DAG, CronDataIntervalTimetable
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_1",
    # Airflow 3 기본(CronTriggerTimetable)은 data_interval_start == end 이므로
    # 전월 말일 ~ 이번 말일 구간을 쓰려면 CronDataIntervalTimetable 사용
    schedule=CronDataIntervalTimetable("10 0 L * *", timezone="Asia/Seoul"),
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    # START_DATE: 전월 말일, END_DATE: 1일 전
    bash_task1 = BashOperator(
        task_id="bash_task1",
        env={
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}'
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )
