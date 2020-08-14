from google.cloud import language_v1
from google.cloud.language_v1 import enums

import json
import datetime

content = '''KB국민은행은 오는 9월 말까지 개인형IRP(퇴직연금)와 연금저축펀드 가입 고객에게 다양한 경품과 모바일 상품권을 증정하는 ‘올~해피(All happy)!’이벤트를 실시한다고 21일 밝혔다.

이번 이벤트는 KB금융그룹 연금사업부문이 공동으로 주관하는 이벤트로 은행·증권의 개인형IRP나 연금저축펀드 신규 및 기존 보유 고객이 참여 가능하며 고객 전원에게 경품을 증정한다.

이벤트 기간 중 개인형IRP와 연금저축펀드를 신규로 가입하거나 계좌 이전한 고객이 이벤트 참여 자산운용사의 추천 펀드 상품에 입금하면 최고 2만원 상당의 ‘CU편의점 모바일상품권’을 증정한다.

또한 기존 개인형IRP나 연금저축펀드 보유 고객이 이벤트 참여 자산운용사의 펀드 상품으로 리밸런싱 하는 경우에도 최고 1만원 상당의 모바일상품권이 제공된다.

이번 이벤트에 참여하는 전 고객을 대상으로 총 93명을 추첨해 ▲삼성 그란데 세탁기·건조기 ▲LG 그램 노트북 ▲LG 코드제로 로봇청소기 ▲다이슨 공기청정기 겸 선풍기 등 푸짐한 경품을 증정하며 신규가입·계좌이전·리밸런싱 고객 대상으로 각각 31명씩 총 세 번 추첨한다.

KB금융지주 윤종규 회장은 “고객행복은 곧 고객자산 확대”라고 강조한 바 있다.

국민은행 관계자는 “이번 이벤트도 다양한 상품을 통해 고객의 세제혜택과 노후 준비를 위해 진행한다”고 말했다.'''

client = language_v1.LanguageServiceClient()
document = {'type':enums.Document.Type.PLAIN_TEXT, 'content': content}

response = client.analyze_entities(document)
#print(entities)

# # Loop through entitites returned from the API
# for entity in response.entities:
#     print(u"Representative name for the entity: {}".format(entity.name))

#     # Get entity type, e.g. PERSON, LOCATION, ADDRESS, NUMBER, et al
#     print(u"Entity type: {}".format(enums.Entity.Type(entity.type).name))

#     # Get the salience score associated with the entity in the [0, 1.0] range
#     print(u"Salience score: {}".format(entity.salience))

#     # Loop over the metadata associated with entity. For many known entities,
#     # the metadata is a Wikipedia URL (wikipedia_url) and Knowledge Graph MID (mid).
#     # Some entity types may have additional metadata, e.g. ADDRESS entities
#     # may have metadata for the address street_name, postal_code, et al.
#     for metadata_name, metadata_value in entity.metadata.items():
#         print(u"{}: {}".format(metadata_name, metadata_value))

#     # Loop over the mentions of this entity in the input document.
#     # The API currently supports proper noun mentions.
#     for mention in entity.mentions:
#         print(u"Mention text: {}".format(mention.text.content))

#         # Get the mention type, e.g. PROPER for proper noun
#         print(
#             u"Mention type: {}".format(enums.EntityMention.Type(mention.type).name)
#         )

# # Get the language of the text, which will be the same as
# # the language specified in the request or, if not specified,
# # the automatically-detected language.
# print(u"Language of the text: {}".format(response.language))

print("------------------------")

def json_default(value): 
    if isinstance(value, datetime.date): 
        return value.strftime('%Y-%m-%d') 
    
    print(value)
    #raise TypeError('not JSON serializable')

#print(json.dumps(response, default=json_default))
print(json.dumps(response, indent=2))