# kb-daas-mvp-dataflow
GCP DataFlow 처리를 위한 소스

## 테스트 방법
1. 가상환경
<PRE>
  $ virtualenv data-sci-env -p python3
  $ source data-sci-env/bin/activate
</PRE>
2. 관련 pip 설치
<PRE>
  $ pip install timezonefinder pytz
  $ pip install apache-beam[gcp]
</PRE>
3. 기타 활성화/비활성화
<PRE>
  $ source env/bin/activate
  $ source deactivate
</PRE>

## 참고문서
1. source : https://github.com/GoogleCloudPlatform/data-science-on-gcp/
2. apache-beam 설명 : https://github.com/sungjunyoung/apache_beam_doc_ko
