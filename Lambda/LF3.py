import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def get_session_id(event):
    query = event['queryStringParameters']
    if query and "sessionId" in query:
        return query["sessionId"]
    return None


def lambda_handler(event, context):
    logger.debug(event)
    # TODO implement
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('old-suggestions')
    session_id = get_session_id(event)
    result = {}
    if session_id is not None:

        response = table.get_item(
            Key={'session_id': session_id}
        )

        logger.debug(response)
        if response and ('Item' in response):
            result['recommendation'] = response['Item']['email']

    return {
        'statusCode': 200,
        'body': json.dumps(result),
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

    }
