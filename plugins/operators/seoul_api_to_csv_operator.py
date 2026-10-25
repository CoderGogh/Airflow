from airflow.sdk import BaseHook, BaseOperator
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path','file_name','base_dt')    # 어떤 필드를 템플릿으로 사용할지 정의(jinja 템플릿 적용)

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)  # 필드값 외의 인자값을 부모 클래스에 전달, 특히 task_id와 dag_id를 전달
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt


    #url : http://url:8080/apikey/type/dataset_nm/start/end
    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id) # connection 정보를 꺼내옴
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        os.makedirs(self.path, exist_ok=True)
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)
        self.log.info(f'저장 완료: {self.path}/{self.file_name} ({len(total_row_df)}건)')

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers=headers)   # String 형태
        contents = json.loads(response.text)    # Json 형태로 변환

        # OpenAPI 응답 정리 -> DataFrame 변환(pandas)
        key_nm = list(contents.keys())[0]
        result = contents.get(key_nm).get('RESULT', contents.get('RESULT', {}))
        # INFO-000: 정상, INFO-200: 데이터 없음 (마지막 페이지 이후 호출 시)
        if result.get('CODE') not in ('INFO-000', 'INFO-200'):
            raise ValueError(f'OpenAPI 오류: {result}')
        row_data = contents.get(key_nm).get('row', [])
        row_df = pd.DataFrame(row_data)

        return row_df
