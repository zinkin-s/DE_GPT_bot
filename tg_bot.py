import json
import requests
import csv
from datetime import datetime
import os
import boto3
import io

URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
folder_id = os.environ.get('FOLDER_ID')
tg_tocken = os.environ.get('TG_TOCKEN')
API_key = os.environ.get('API_KEY')

def handler(event, context):
    
    try:
        data = json.loads(event.get('body'))
        print(data)

        chat_id = data['message']['chat']['id']
        
        text = data['message']['text']

        msg = '' + text
        
        try:
            ydb_doc_api_client = boto3.resource(
                'dynamodb',
                endpoint_url = 'https://docapi.serverless.yandexcloud.net/ru-central1/b1g37i3tjbefiamht52d/etn4rq6puh5giq1b7c70'
            ) 
            
            table = ydb_doc_api_client.Table('slslogapi')
            curtime = datetime.now()
            curtime = int(curtime.timestamp() * 1000)
            
        except Exception as e:
            print(e)

        resp = requests.post(
            URL,
            headers = {
                'Authorization': f'Api-Key {API_key}',
                'x-folder-id': f'{folder_id}'
            },
            json = {
                'modelUri': f"gpt://{folder_id}/yandexgpt",
                'completionOptions': {"temperature": 0.6, "maxTokens": 2000},
                'messages': [{
                    'role': 'system',
                    'text': msg
                }]
            }
        ).json()
        answer = resp['result']['alternatives'][0]['message']['text']
        input_text_tockens = resp['result']['usage']['inputTextTokens']
        completion_tockens = resp['result']['usage']['completionTokens']
        total_tockens = resp['result']['usage']['totalTokens']

        response = table.put_item(
                Item={
                    'id': curtime,
                    'user_id': data['message']['from']['id'],
                    'action': 'start' if '/start' in data['message']['text'] else 'answer',
                    'chat_id': chat_id,
                    'GPT_request': msg,
                    'GPT_answer': answer,
                    'input_text_tockens': input_text_tockens,
                    'completion_tockens': completion_tockens,
                    'total_tockens': total_tockens
                }
            )
        requests.post(
        f'https://api.telegram.org/bot{tg_tocken}/sendMessage',
        json = {
            'chat_id': chat_id,
            'text': answer
            }
        )
    except Exception as e:
        answer = e
 
    return {
        'statusCode': 200,
        'body': '!',
    }