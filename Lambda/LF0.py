import boto3
import uuid
import logging

# Define the client to interact with Lex
import botocore

client = boto3.client('lexv2-runtime')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.debug(event)
    # print(event)
    msg_from_user = event['messages'][0]
    if "sessionId" not in event or not event["sessionId"]:
        sessionId = str(uuid.uuid4())
    else:
        sessionId = event['sessionId']

    response = client.recognize_text(botId='KAQRSIZH6T',
                                     botAliasId='QAPDYFIEFO',
                                     localeId='en_US',
                                     text=msg_from_user['unstructured']['text'],
                                     sessionId=sessionId)

    msgs_from_lex = response['messages']
    if msgs_from_lex:
        result_text = ""
        for message in msgs_from_lex:
            result_text += message['content']

        resp = {
            'statusCode': 200,
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": "1",
                        "text": result_text,
                        "timestamp": "10"
                    }
                }
            ],
            "sessionId": sessionId
        }
        logger.debug(resp)
        # print(resp)
        return resp