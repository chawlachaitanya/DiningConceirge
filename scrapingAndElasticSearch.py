import requests
import json
import pandas as pd
from pprint import pprint
import boto3

from decimal import Decimal
import datetime
import time

from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch
from opensearchpy import OpenSearch, RequestsHttpConnection

API_KEY = 'mRYpxyK7lLy4W9Fmy1Cqi3UaRPYz3f6UMfzbVB7-HKUIX6lH_yejOP8PLEfFP3FiyocjWRKqwPC_qi3GZJro4Qv9t1f8mItBPlsj0aa647lEfrNgT2idadim1FoRYnYx'


def add_item_to_dynamodb(table, row):
    current_time = time.time()
    response = table.put_item(
        Item={
            'BusinessId': row["id"],
            'name': row["name"],
            'address1': row["location"]["address1"],
            'address2': row["location"]["address2"],
            'address3': row["location"]["address3"],
            'zip_code': row["location"]["zip_code"],
            'coordinates': row["coordinates"],
            'review_count': row["review_count"],
            'rating': row["rating"],
            "insertedAtTimestamp": datetime.datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S'),
            'cuisine': row["cuisine"]
        }
    )
    return response


def add_restaurants_to_dynamodb(restaurants_df):
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
    table = dynamodb.Table('yelp-restaurants')
    counter = 0
    waiting = [0, 100, 500, 1000, 2000]
    for row in restaurants_df:
        for wait in waiting:
            try:
                add_item_to_dynamodb(table, row)
                print(counter)
                counter += 1
                break
            except BaseException as ex:
                print(ex.args)
                time.sleep(wait / 1000)


def create_es_json(restaurants_df):
    lines = []
    for row in restaurants_df:
        restaurant = {"RestaurantID": row["id"], "Cuisine": row["cuisine"]}
        temp = {"_index": "restaurants", "_id": row["id"]}
        index = {"index": temp}
        lines.append(json.dumps(index) + "\n")
        lines.append(json.dumps(restaurant) + "\n")

    with open("restaurants.json", "w") as f:
        f.writelines(lines)


def fetch_restaurants():
    url = 'https://api.yelp.com/v3/businesses/search'
    cuisines = ['italian', 'chinese', 'indian', 'french', 'thai', 'japanese', 'mexican', 'korean']
    location = 'manhattan'
    results_per_page = 50
    headers = {'Authorization': 'Bearer {}'.format(API_KEY)}
    results = []
    for cuisine in cuisines:
        offset = 0
        temp_results = []
        print(cuisine)
        while offset <= 700:
            url_params = {'term': cuisine, 'location': location.replace(' ', '+'),
                          'limit': results_per_page,
                          'offset': offset}
            response = json.loads(requests.get(url, headers=headers, params=url_params).text)
            temp_results.extend(response["businesses"])
            total = response["total"]
            offset = offset + 50
            if offset + 50 > total:
                break

        for business in temp_results:
            business['cuisine'] = cuisine
        results.extend(temp_results)
    restaurants_df = pd.DataFrame(results).drop_duplicates(subset=['id'])
    restaurants_df = json.loads(json.dumps(restaurants_df.to_dict('records')), parse_float=Decimal)
    return restaurants_df


restaurants = fetch_restaurants()
print(len(restaurants))
add_restaurants_to_dynamodb(restaurants)
create_es_json(restaurants)


# curl -XPOST -u 'chaitanya:Columbia@2022' 'https://search-restaurants-ptq3spa5hb5qsksrkav47x4rxq.us-east-1.es.amazonaws.com/_bulk' --data-binary @restaurants.json -H 'Content-Type: application/json'
