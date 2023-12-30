import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from database_utils import AWSDBConnector


random.seed(100)

aws_dbconnector = AWSDBConnector("db_creds.yaml")

def fetch_row(table_name:str, offset:int):
    """Fetch a row from the table at the given `offset`

    Arguments:    
        table_name -- str
            Name of the table to extract a data row from
    Returns:
        data as `dict`
    """
    sql = text(f"SELECT * FROM {table_name} LIMIT 1 OFFSET {offset}")
    
    engine = aws_dbconnector.create_db_connector()
    with engine.connect() as connection:
        selected_rows = connection.execute(sql)
    
    return selected_rows.first()._mapping


def fetch_post_data():
    """
    Fetch and return 1 row from all 3 tables
    pinterest_data    - contains data about posts being updated to Pinterest
    geolocation_data  - contains data about the geolocation of each Pinterest post found in pinterest_data
    user_data         - contains data about the user that has uploaded each post found in pinterest_data
    """
    random_row = random.randint(0, 11000)
    pin_result = dict(fetch_row(table_name="pinterest_data", offset=random_row))
    geo_result = dict(fetch_row(table_name="geolocation_data", offset=random_row))
    user_result = dict(fetch_row(table_name="user_data", offset=random_row))
    
    return pin_result, geo_result, user_result


def prepare_payload(data:dict):
    """Convert dict to JSON serialized record

    Arguments:
        data -- content or geolocation or user data for a post

    Returns:
        serialized JSON string
    """
    return json.dumps({
        "records": [{ "value": data }]
    }, default=lambda d: d.isoformat())  # convert datetime to serializable isoformat


def send_data(data:dict, topic:str):
    """Send data to kafka topic

    Arguments:
        data -- topic's data as dictionary
        topic -- name of topic to send data to
    """
    payload = prepare_payload(data=data)

    # Invoke URL for the API deployed in API Gateway to send data to MSK cluster 
    invoke_url = r"https://hh632okm50.execute-api.us-east-1.amazonaws.com/prod/topics/{topic}/"
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    url = invoke_url.format(topic=topic)
    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"{response.status_code} : {response.content}")


def send_one_post():
    """Fetch data from the 3 tables on the RDS instance and
    send it to the corresponding kafka topics on the MSK cluster
    """
     # Topics created on the MSK cluster:
    topics = ['12e371d757c1.pin', '12e371d757c1.geo', '12e371d757c1.user']

    for post in zip(fetch_post_data(), topics):
        send_data(post[0], post[1])


def run_infinite_post_data_loop():
    """Simulate sending posts on Pinterest
    """
    while True:
        sleep(random.randrange(0, 2))
        send_one_post()


if __name__ == "__main__":
    print('Working...')
    # run_infinite_post_data_loop()
    # Send 1100 records for controlled cleaning verification
    for _ in range(1100):
        send_one_post()