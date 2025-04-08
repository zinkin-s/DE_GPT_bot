import boto3
from boto3.dynamodb.conditions import Key
import datetime
import pandas as pd
import io


def handler(event, context):
    try:
        ydb_doc_api_client = boto3.resource(
                'dynamodb',
                endpoint_url = 'https://docapi.serverless.yandexcloud.net/ru-central1/b1g37i3tjbefiamht52d/etn4rq6puh5giq1b7c70'
            ) 
            
        table = ydb_doc_api_client.Table('slslogapi')
        
        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days=1)

        response = table.scan(
        FilterExpression='id >= :yesterday',
        ExpressionAttributeValues={':yesterday': int(yesterday.timestamp() * 1000)}
        )
        
        transactions = response['Items']

        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression='id >= :yesterday',
                ExpressionAttributeValues={':yesterday': int(yesterday.timestamp() * 1000)},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            transactions.extend(response['Items'])

        df = pd.DataFrame(transactions)
        filename = 'log_' + yesterday.strftime('%Y-%m-%d')

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3', 
            endpoint_url='https://storage.yandexcloud.net/'
        )
        
        csv_data = df.to_csv(index=False)

        s3_client.put_object(Key=f'{filename}.csv',Bucket='for-tg-bot-st', Body=csv_data)
    except Exception as e:
        print(e)

    return {
        'statusCode': 200,
        'body': 'Ok',
    }