# kb-daas-mvp-dataflow
GCP DataFlow 처리를 위한 소스 ()

## 테스트 방법
1. 가상환경
<PRE>
  $ cd ~
  $ virtualenv daas-env -p python3
  $ source daas-env/bin/activate
</PRE>
2. 관련 pip 설치
<PRE>
  $ pip install timezonefinder pytz
  $ pip install apache-beam[gcp]
</PRE>
3. 기타 활성화/비활성화
<PRE>
  $ cs ~
  $ source daas-env/bin/activate
  $ deactivate
</PRE>

## 참고문서
1. source : https://github.com/GoogleCloudPlatform/data-science-on-gcp/
2. apache-beam 설명 : https://github.com/sungjunyoung/apache_beam_doc_ko
3. Data Flow Prediction 예제 : https://github.com/GoogleCloudPlatform/dataflow-prediction-example
4. Data Flow Template (Java) : https://github.com/GoogleCloudPlatform/DataflowTemplates.git
5. Data Flow Example (Python) : https://github.com/GoogleCloudPlatform/professional-services/tree/master/examples/dataflow-python-examples


## 구글독스 정리 
1. GCS에서 빅쿼리로 csv적재 : https://docs.google.com/document/d/1RXJESagdBChFIA6W863GIUYbH6Qnn_UYzq6I_Uk-eIk/edit?usp=sharing
2. 빅쿼리 select > ai > insert : https://docs.google.com/document/d/1QepjUftoFVncGRlAv_KHJgmOHN0DRezLp-J5tFDpA0A/edit?usp=sharing
