import json
import boto3
from botocore.exceptions import ClientError
import urllib3

dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")


def get_restaurant_ids(cuisine):
    http = urllib3.PoolManager()

    url = 'https://search-restaurants-ptq3spa5hb5qsksrkav47x4rxq.us-east-1.es.amazonaws.com/restaurants/_search'

    headers = urllib3.util.request.make_headers(basic_auth='chaitanya:Columbia@2022')
    headers["Content-Type"] = "application/json"
    headers["Accept"] = "application/json"
    query = {
        "size": 10,
        "query": {
            "function_score": {
                "random_score": {},
                "query": {"term": {"Cuisine": cuisine}},
            }
        }
    }

    resp = http.request("GET", url, headers=headers, body=json.dumps(query))

    # Print the returned data.
    data = json.loads(resp.data)

    restaurant_list = data['hits']['hits']
    result_ids = [r['_source']['RestaurantID'] for r in restaurant_list]
    return result_ids


def get_restaurant_information(restaurant_ids):
    table = dynamodb.Table('yelp-restaurants')

    restaurants = []

    for restaurant_id in restaurant_ids:
        response = table.get_item(
            Key={
                'BusinessId': restaurant_id
            }
        )
        if response and ('Item' in response):
            restaurants.append(response['Item'])

    return restaurants


def create_message(restaurant_details, query):
    message = f'''Hello! Here are my {query['cuisine']} restaurant suggestions for {query['numberOfPeople']} people, for {query['date']} at {query['time']}:'''
    message += "\n<br><br>"
    for restaurant in restaurant_details:
        name = restaurant['name']
        temp = [restaurant['address1'], restaurant['address2'], restaurant['address3'], restaurant['zip_code']]
        address = ""
        for item in temp:
            if item:
                address += item
        message += f"<br><br> {name}, located at {address}"

    return message


def send_email(message, recipient_email):
    sender = "chaitanyachawla1996@gmail.com"
    aws_region = "us-east-1"
    subject = "Restaurant Details"
    body_text = message

    # The HTML body of the email.
    body_html = f"""<html>
    <head></head>
    <body>
      <p>{message}</p>
    </body>
    </html>
                """

    # The character encoding for the email.
    charset = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses', region_name=aws_region)

    # Try to send the email.
    try:
        # Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    recipient_email,
                ],
            },
            Message={
                'Body':
                    {
                        'Text': {
                            'Charset': charset,
                            'Data': body_text,
                        },
                        'Html': {
                            'Charset': charset,
                            'Data': body_html,

                        },
                    },

                'Subject': {
                    'Charset': charset,
                    'Data': subject,
                },
            },
            Source=sender
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


# Create SQS client
def get_and_process_message():
    sqs = boto3.client('sqs')

    queue_url = 'https://sqs.us-east-1.amazonaws.com/197953848710/RestaurantSearch'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=60,
        WaitTimeSeconds=0
    )

    if 'Messages' not in response.keys() or len(response['Messages']) == 0:
        return

    for message in response['Messages']:
        body = message['Body']
        print(body)
        query = json.loads(body)
        receipt_handle = message['ReceiptHandle']

        # This should come from elastic search
        restaurant_ids = get_restaurant_ids(query['cuisine'])
        if restaurant_ids is None or len(restaurant_ids) == 0:
            print('No restaurants found')
            continue

        restaurant_infos = get_restaurant_information(restaurant_ids)
        email_message = create_message(restaurant_infos, query)
        print(email_message)

        send_email(email_message, query['email'])
        if "sessionId" in query:
            table = dynamodb.Table('old-suggestions')
            response = table.put_item(
                Item={
                    'session_id': query["sessionId"],
                    'email': email_message
                }
            )
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )


def lambda_handler(event, context):
    # TODO implement
    get_and_process_message()
    return {
        'statusCode': 200
    }
