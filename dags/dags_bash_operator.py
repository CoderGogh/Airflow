from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *", ## 분 시 일 월 요일
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,  ## 이전 과거 미실행 기간의 백필 여부
    tags=["example", "example2", "example3"],   ## 옵션
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",  ## 테스크 그래프에 표시될 이름
        bash_command="echo 'Who am I?'",
    )

    bash_t2 = BashOperator(
            task_id="bash_t2",  ## 테스크 그래프에 표시될 이름
            bash_command="echo $HOSTNAME",
        )

    bash_t1 >> bash_t2  ## 실행순서