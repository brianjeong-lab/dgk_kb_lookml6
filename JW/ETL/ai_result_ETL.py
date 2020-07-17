# Construct a BigQuery client object.
from google.cloud import bigquery
import requests

### 설정값
client = bigquery.Client(project='kb-daas-dev')
query = """
    SELECT ID, D_WRITESTAMP, D_CONTENT FROM `kb-daas-dev.raw_data.rsn_bank` WHERE DATE(D_WRITESTAMP) = "2020-05-30" LIMIT 1000
"""
# 가져올 쿼리
table_id = "kb-daas-dev.ai_result_data.ai_result_bank" # 적재할 테이블명
table = client.get_table(table_id)  # Make an API request.
###

# 불필요한 특수문자 제거 전처리 함수
def preprocess_contents(contents):
    remove_target_char = ["&", "="] # 본문에서 제거해야할 특수문자
    for char in remove_target_char:
        contents = contents.replace(char, " ")
    return contents


# ai api 콜 함수 # content에 분석할 내용 담기
def call_request_ai_api(content):
    url = "http://3.34.18.1:8080/analyze"
    querystring = {"tasks":"d2c,kpe,kse"}
    body = "text=" + content
    body = body.encode(encoding='utf-8')
    headers = {
        'apikey': "5rbeC7bMzbynvbcNqGwOnp5Tll2PUB9B",
        'Content-Type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache"
        }
    response = requests.request("POST", url, data=body, headers=headers, params=querystring)
    return response.text

# 빅쿼리 insert 함수
def export_items_to_bigquery(id, writestamp, result):

    # insert할 row만들기
    rows_to_insert = [
        (id, writestamp, result)
    ]

    errors = client.insert_rows(table, rows_to_insert)  # API request
    if errors == []:
        print("New rows have been added.")
    print(errors)

if __name__ == "__main__":

    # 빅쿼리 쿼리 요청
    query_job = client.query(query)  # Make an API request.

    cnt_num = 0
    for row in query_job:
        cnt_num = cnt_num + 1
        print("쿼리 처리 번호: " + str(cnt_num))
        clean_contents = preprocess_contents(row["D_CONTENT"])
        result = call_request_ai_api(content = clean_contents)
        export_items_to_bigquery(row["ID"], str(row["D_WRITESTAMP"]), result)
