import json
import requests
import csv
from datetime import datetime
import os
import boto3
import io

URL = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion" # URL модели
folder_id = os.environ.get('FOLDER_ID') # folder_id облака
tg_tocken = os.environ.get('TG_TOCKEN') # telegram tocken
API_key = os.environ.get('API_KEY') # API ключ

def handler(event, context):
    """Обработчик запроса serverless-функции telegram-бота
    Бот реализован в виде serverless-функции. Логирование проводится в базу данных YDB
    Используется вариант документной базы данных, так как это позвояет расширять количество полей в БД

    Args:
        event (json): тело запроса, JSON-документ 
        context: контекст вызова

    Returns:
        dict
    """
    
    try:
        data = json.loads(event.get('body')) # загрузка json-запроса
        print(data)

        chat_id = data['message']['chat']['id'] # chat_id запроса к боту
        
        text = data['message']['text'] # текст запроса к боту

        msg = '' + text # текст отправляемый к gpt
        
        try:
            # подключение к БД
            ydb_doc_api_client = boto3.resource(
                'dynamodb',
                endpoint_url = 'https://docapi.serverless.yandexcloud.net/ru-central1/b1g37i3tjbefiamht52d/etn4rq6puh5giq1b7c70'
            ) 
            # загрузка таблицы
            table = ydb_doc_api_client.Table('slslogapi')
            # получение текущего времени
            curtime = datetime.now()
            curtime = int(curtime.timestamp() * 1000) # создание timestamp в миллисекундах
            
        except Exception as e:
            print(e)

        # создание запроса к gpt
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
        
        answer = resp['result']['alternatives'][0]['message']['text'] # ответ gpt
        input_text_tockens = resp['result']['usage']['inputTextTokens'] # количество токенов в исходящем запросе
        completion_tockens = resp['result']['usage']['completionTokens'] # количество токенов в ответе
        total_tockens = resp['result']['usage']['totalTokens'] # общее количество токенов

        # запись лога в БД
        response = table.put_item(
                Item={
                    'id': curtime,
                    'user_id': data['message']['from']['id'], # запись user_id 
                    'action': 'start' if '/start' in data['message']['text'] else 'answer',
                    'chat_id': chat_id,
                    'GPT_request': msg,
                    'GPT_answer': answer,
                    'input_text_tockens': input_text_tockens,
                    'completion_tockens': completion_tockens,
                    'total_tockens': total_tockens
                }
            )
        
        # отправка сообщения телеграм-боту
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