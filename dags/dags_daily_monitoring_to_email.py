from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum
from contextlib import closing
import os
import pandas as pd
from airflow.sdk import DAG, task

# 수신자 (.env, 콤마로 여러 명 지정 가능)
email_lst = [email.strip() for email in os.environ['REDIRECT_EMAIL'].split(',')]

with DAG(
    dag_id='dags_daily_dag_monitoring_to_email',
    start_date=pendulum.datetime(2023,5,1, tz='Asia/Seoul'),
    schedule='0 8 * * *',
    catchup=False
) as dag:
    
    @task(task_id='get_daily_monitoring_rslt_task')
    def get_daily_monitoring_rslt_task(**kwargs):
        def fmt(dt):
            return '없음' if pd.isna(dt) else pd.Timestamp(dt).strftime('%Y-%m-%d %H:%M')

        postgres_hook = PostgresHook(postgres_conn_id='conn_db_postgres_airflow')
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                with open('/opt/airflow/files/sqls/daily_dag_monitoring.sql', 'r') as sql_file:
                    cursor.execute("SET TIME ZONE 'Asia/Seoul';")      # 조회되는 시각을 KST 로 표시
                    sql = sql_file.read()
                    cursor.execute(sql)
                    rslt = cursor.fetchall()
                    rslt = pd.DataFrame(rslt, columns=['dag_id','run_cnt','success_cnt','failed_cnt','running_cnt','last_failed_date','last_success_date','running_date','next_dagrun'])
                    html_content = ''

                    # 1) 실패대상
                    failed_df = rslt.query("(failed_cnt > 0)")
                    html_content += "<h2>2 실패 대상</h2><br/>"
                    if not failed_df.empty:
                        for idx, row in failed_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>최근 실패일자: {fmt(row['last_failed_date'])}<br/>마지막 성공일자: {fmt(row['last_success_date'])}<br/><br/>"
                    else:
                        html_content += "없음<br/><br/>"


                    # 2) 미수행 대상
                    skipped_df = rslt.query("(run_cnt == 0)")
                    html_content += "<h2>3 미수행 대상</h2><br/>"
                    if not skipped_df.empty:
                        for idx, row in skipped_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>예정일자: {fmt(row['next_dagrun'])}<br/><br/>"
                    else:
                        html_content += "없음<br/><br/>"

                    # 3) 수행 중 대상
                    running_df = rslt.query("(running_cnt > 0)")
                    html_content += "<h2>4 수행 중</h2><br/>"
                    if not running_df.empty:
                        for idx, row in running_df.iterrows():
                            html_content += f"DAG: {row['dag_id']}<br/>배치일자: {fmt(row['running_date'])}<br/><br/>"
                    else:
                        html_content += "없음<br/><br/>"

                    # 4) 성공 대상
                    done_success_cnt = rslt.query("(failed_cnt == 0) and (run_cnt > 0) and (running_cnt == 0)").shape[0]
                    yesterday = pendulum.yesterday('Asia/Seoul').strftime('%Y-%m-%d')
                    now = pendulum.now('Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S')

                    ti = kwargs['ti']
                    ti.xcom_push(key='subject', value=f"DAG 수행현황 알람({yesterday} ~ {now})")
                    html_content = f'''<h1>DAG 수행현황 알람({yesterday} ~ {now})</h1><br/><br/>
<h2>1. 수행 대상 DAG 개수: {rslt.shape[0]}</h2><br/>
&nbsp;&nbsp;&nbsp;&nbsp;(1) 성공 DAG 개수: {done_success_cnt}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(2) 실패: {failed_df.shape[0]}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(3) 미수행: {skipped_df.shape[0]}<br/>
&nbsp;&nbsp;&nbsp;&nbsp;(4) 수행 중: {running_df.shape[0]}<br/><br/>''' + html_content
                    
                    print(html_content)
                    return html_content

    send_email = EmailOperator(
        task_id='send_email',
        conn_id='conn_smtp_gmail',
        to=email_lst,
        # Airflow 3 는 task_ids 생략 시 자기 자신(send_email)의 XCom 을 조회하므로 명시 필요
        subject="{{ ti.xcom_pull(task_ids='get_daily_monitoring_rslt_task', key='subject') }}",
        html_content="{{ ti.xcom_pull(task_ids='get_daily_monitoring_rslt_task') }}"
    )

    get_daily_monitoring_rslt_task() >> send_email