import math
from datetime import datetime, timedelta
import json
import boto3
import re
import logging

sqs_client = boto3.client('sqs')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def lambda_handler(event, context):
    logger.debug(event)
    intent = getIntent(event)
    intent_name = getIntentName(intent)
    if intent_name == 'DiningSuggestionsIntent':
        slots = getSlots(intent)
        logger.debug(slots)
        if event['invocationSource'] == 'DialogCodeHook':
            return validate_dinning_recommendation(slots, intent_name)

        if event['invocationSource'] == 'FulfillmentCodeHook':
            sessionId = getSessionId(event)
            message = getSQSMessage(sessionId, slots)
            sendMessageToSQS(message)
            return close(intent_name, "That's amazing, I will get back to you with my suggestions shortly.")

    logger.debug("Intent %s not handled", intent_name)
    return {}


def elicit_slot(slot_to_elicit, message, slots, intent_name):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit
            },
            "intent": {
                "name": intent_name,
                "slots": slots
            }
        },
        "messages": [
            {
                "contentType": "SSML",
                "content": message,

            }]
    }


def close(intent, context):
    return {
        "sessionState": {
            "dialogAction": {
                "type": "Close"
            },
            "intent": {
                "name": intent,
                "state": "Fulfilled"
            },
            "messages": [
                {
                    "contentType": "PlainText",
                    "content": context
                }
            ]
        }
    }


def delegate(slots, intent_name):
    response = {
        "sessionState": {
            "dialogAction": {
                "type": "Delegate"
            },
            "intent": {
                "name": intent_name,
                "slots": slots
            }
        }
    }
    logger.debug(response)
    return response


def isValidLocation(location_slot):
    location = getValueForSlot(location_slot)
    if location:
        logger.debug(location)
        return location in ['bronx', 'brooklyn', 'manhattan', 'queens', 'staten island', 'new york']
    return False


def isValidCuisine(cuisine_slot):
    cuisine_value = getValueForSlot(cuisine_slot)
    if cuisine_value:
        logger.debug(cuisine_value)
        return cuisine_value in ['italian', 'chinese', 'indian', 'french', 'thai', 'japanese', 'mexican', 'korean']
    return False


def isValidDiningDate(dining_date_slot):
    date = getValueForSlot(dining_date_slot)
    if date:
        booking_timestamp = datetime.strptime(date + ' ' + '23:59', '%Y-%m-%d %H:%M').astimezone()
        logger.debug(booking_timestamp)
        now = datetime.now().astimezone() - timedelta(hours=5)
        return now < booking_timestamp
    return False


def isValidDiningTime(dining_time_slot, dining_date_slot):
    time = getValueForSlot(dining_time_slot)
    if time:
        date = getValueForSlot(dining_date_slot)
        booking_timestamp = datetime.strptime(date + ' ' + time, '%Y-%m-%d %H:%M').astimezone()
        logger.debug(booking_timestamp)
        now = datetime.now().astimezone() - timedelta(hours=5)
        return now < booking_timestamp
    return False


def isValidNumberOfPeople(number_of_people_slot):
    number_of_people = getValueForSlot(number_of_people_slot)
    if number_of_people:
        logger.debug(number_of_people)
        return 1 <= parse_int(number_of_people) <= 10
    return False


def isEmailValidate(email_slot):
    email = getValueForSlot(email_slot)
    if email:
        logger.debug(email)
        regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        return re.fullmatch(regex, email)
    return False


def validate_dinning_recommendation(slots, intent_name):
    location_slot = slots['Location']
    if location_slot:
        if not isValidLocation(location_slot):
            return elicit_slot('Location',
                               "Sorry, I can only recommend resturants in Manhattan. Can I suggest you something around Manhattan?",
                               slots, intent_name)

    cuisine_slot = slots['Cuisine']
    if cuisine_slot:
        if not isValidCuisine(cuisine_slot):
            return elicit_slot('Cuisine',
                               "Sorry, I did not recognize that. What cuisine would you like to try?",
                               slots, intent_name)

    number_of_people_slot = slots['NumberOfPeople']
    if number_of_people_slot:
        if not isValidNumberOfPeople(number_of_people_slot):
            return elicit_slot('NumberOfPeople',
                               "Sorry, I can only provide suggestions for up to 10 people. How many people are you looking a table for?",
                               slots, intent_name)

    dining_date_slot = slots['DiningDate']
    if dining_date_slot:
        if not isValidDiningDate(dining_date_slot):
            return elicit_slot('DiningDate', 'Sorry, I did not recognize that. What date to do you want to eat?', slots,
                               intent_name)

    dining_time_slot = slots['DiningTime']
    if dining_time_slot:
        if not isValidDiningTime(dining_time_slot, dining_date_slot):
            return elicit_slot('DiningTime', 'Sorry, I did not recognize that. What time to do you want to eat?', slots,
                               intent_name)

    email_slot = slots['EmailAddress']
    if email_slot:
        if not isEmailValidate(email_slot):
            return elicit_slot('EmailAddress',
                               "Sorry, that isn't a valid email. What email address I can reach you at?",
                               slots, intent_name)

    return delegate(slots, intent_name)


def sendMessageToSQS(message):
    response = sqs_client.send_message(
        QueueUrl='https://sqs.us-east-1.amazonaws.com/197953848710/RestaurantSearch',
        MessageBody=json.dumps(message),
    )
    logger.debug(response)


def getSlots(intent):
    return intent['intent']['slots']


def getIntentName(intent):
    return intent['intent']['name']


def getIntent(event):
    return event['interpretations'][0]


def getValueForSlot(slot):
    if len(slot['value']['resolvedValues']):
        return slot['value']['resolvedValues'][0].lower()
    return None


def parse_int(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')


def getSessionId(event):
    return event['sessionId']


def getSQSMessage(sessionId, slots):
    return {
        "sessionId": sessionId,
        "location": getValueForSlot(slots['Location']),
        "email": getValueForSlot(slots['EmailAddress']),
        "numberOfPeople": getValueForSlot(slots['NumberOfPeople']),
        "date": getValueForSlot(slots['DiningDate']),
        "time": getValueForSlot(slots['DiningTime']),
        "cuisine": getValueForSlot(slots['Cuisine'])
    }
