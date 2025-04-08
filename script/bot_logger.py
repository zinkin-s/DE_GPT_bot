import boto3
import datetime
import pandas as pd
import io
import os

endpoint_url = os.environ.get('ENDPOINT_URL') # endpoint_url базы данных


def handler(event, context):
    """sereverless-функция для записи логов в формате csv в Object Storage
    Запись проводится 1 раз в сутки по триггеру

    Args:
        event (json): тело запроса, JSON-документ 
        context: контекст вызова

    Returns:
        dict
    """
    try:
        # подключение к БД
        ydb_doc_api_client = boto3.resource(
                'dynamodb',
                endpoint_url = endpoint_url
            ) 
         
         # подключение к таблице с данными   
        table = ydb_doc_api_client.Table('slslogapi')
        
        today = datetime.datetime.now() # получение сегодняшней даты
        yesterday = today - datetime.timedelta(days=1) # получение даты предыдущих суток

        # запрос к БД и получение данных за прошедшие сутки
        response = table.scan(
            FilterExpression='id >= :yesterday',
            ExpressionAttributeValues={':yesterday': int(yesterday.timestamp() * 1000)}
        )
        
        # получение всех транзакций за сутки
        transactions = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='id >= :yesterday',
                ExpressionAttributeValues={':yesterday': int(yesterday.timestamp() * 1000)},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            transactions.extend(response['Items'])

        df = pd.DataFrame(transactions) # датафрейм с полученными данными
        filename = 'log_' + yesterday.strftime('%Y-%m-%d') # маска имени файла

        # создание сессии подключения к объектному хранилищу S3
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3', 
            endpoint_url='https://storage.yandexcloud.net/'
        )
        
        csv_data = df.to_csv(index=False) # преобразование данных в csv

        # отправка csv в объектное хранилище
        s3_client.put_object(Key=f'{filename}.csv',Bucket='for-tg-bot-st', Body=csv_data)
    except Exception as e:
        print(e)

    return {
        'statusCode': 200,
        'body': 'Ok',
    }