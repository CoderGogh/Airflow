from airflow.sdk import BaseHook
from urllib.parse import quote_plus
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self): # 재정의
        # get_connection : Airflow UI와 연결
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id) # Class method type
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        #DB와의 연결
        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn   # DB session 객체


    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn().close()  # Connection 정보만 사용하고 psycopg2 연결은 닫음
        header = 0 if is_header else None                       # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append'       # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')      # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            # 숫자형일 경우
            except:
                continue

        self.log.info('적재 건수:' + str(len(file_df)))
        # 비밀번호의 특수문자(@, :, / 등)가 URI를 깨뜨리지 않도록 인코딩
        uri = f'postgresql://{quote_plus(self.user)}:{quote_plus(self.password)}@{self.host}:{self.port}/{self.dbname}'
        engine = create_engine(uri) # sqlalchemy engine
        file_df.to_sql(name=table_name,
                            con=engine,
                            schema='public',
                            if_exists=if_exists,
                            index=False
                        )
