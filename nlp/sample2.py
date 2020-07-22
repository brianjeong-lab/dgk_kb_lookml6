#!/usr/bin/env python

# Copyright 2016 Google, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Analyzes text using the Google Cloud Natural Language API."""

import argparse
import json
import sys

import googleapiclient.discovery


def get_native_encoding_type():
    """Returns the encoding type that matches Python's native strings."""
    if sys.maxunicode == 65535:
        return 'UTF16'
    else:
        return 'UTF32'


def analyze_entities(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding,
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeEntities(body=body)
    response = request.execute()

    return response


def analyze_sentiment(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeSentiment(body=body)
    response = request.execute()

    return response


def analyze_syntax(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeSyntax(body=body)
    response = request.execute()

    return response

content = '''KB국민은행은 오는 9월 말까지 개인형IRP(퇴직연금)와 연금저축펀드 가입 고객에게 다양한 경품과 모바일 상품권을 증정하는 ‘올~해피(All happy)!’이벤트를 실시한다고 21일 밝혔다.
이번 이벤트는 KB금융그룹 연금사업부문이 공동으로 주관하는 이벤트로 은행·증권의 개인형IRP나 연금저축펀드 신규 및 기존 보유 고객이 참여 가능하며 고객 전원에게 경품을 증정한다.
'''

# entities
entities = analyze_entities(content, get_native_encoding_type())

"""
{
  "entities": [
    {
      "name": "KB\uae08\uc735\uadf8\ub8f9 \uc5f0\uae08",
      "type": "ORGANIZATION",
      "metadata": {},
      "salience": 0.058673654,
      "mentions": [
        {
          "text": {
            "content": "KB\uae08\uc735\uadf8\ub8f9 \uc5f0\uae08",
            "beginOffset": 115
          },
          "type": "PROPER"
        }
      ]
    },
"""
print((json.dumps(entities["entities"], indent=2)))

# sentiment
sentiment = analyze_sentiment(content, get_native_encoding_type())
"""
{
  "documentSentiment": {
    "magnitude": 2.6,
    "score": 0.3
  },
  "language": "ko",
  "sentences": [
    {
      "text": {
        "content": "KB\uad6d\ubbfc\uc740\ud589\uc740 \uc624\ub294 9\uc6d4 \ub9d0\uae4c\uc9c0 \uac1c\uc778\ud615IRP(\ud1f4\uc9c1\uc5f0\uae08)\uc640 \uc5f0\uae08\uc800\ucd95\ud380\ub4dc \uac00\uc785 \uace0\uac1d\uc5d0\uac8c \ub2e4\uc591\ud55c \uacbd\ud488\uacfc \ubaa8\ubc14\uc77c \uc0c1\ud488\uad8c\uc744 \uc99d\uc815\ud558\ub294 \u2018\uc62c~\ud574\ud53c(All happy)!\u2019\uc774\ubca4\ud2b8\ub97c \uc2e4\uc2dc\ud55c\ub2e4\uace0 21\uc77c \ubc1d\ud614\ub2e4.",
        "beginOffset": 0
      },
      "sentiment": {
        "magnitude": 0.3,
        "score": 0.3
      }
    },
"""
#print((json.dumps(sentiment, indent=2)))