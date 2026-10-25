import os
from pathlib import Path
import requests
from dotenv import load_dotenv


# plugins/ 아래 .py는 Airflow가 task마다 import하므로, 직접 실행할 때만 토큰 요청
if __name__ == '__main__':
    # Mac에서 직접 실행할 때 repo 루트의 .env 로드 (컨테이너에서는 env_file로 이미 주입됨)
    load_dotenv(Path(__file__).resolve().parents[2] / '.env')

    client_id = os.environ['KAKAO_CLIENT_ID']
    client_secret = os.environ['KAKAO_CLIENT_SECRET']
    redirect_uri = os.environ.get('KAKAO_REDIRECT_URI', 'https://example.com/oauth')
    authorize_code = os.environ['KAKAO_AUTHORIZE_CODE']


    token_url = 'https://kauth.kakao.com/oauth/token'
    data = {
        'grant_type': 'authorization_code',
        'client_id': client_id,
        'client_secret': client_secret,
        'redirect_uri': redirect_uri,
        'code': authorize_code,
        }

    response = requests.post(token_url, data=data, timeout=10)
    tokens = response.json()
    print(tokens)
