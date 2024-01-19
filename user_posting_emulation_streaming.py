import requests
import json

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


def send_data(data:dict, stream_name:str):
    """Send data to stream

    Arguments:
        data -- stream's data as dictionary
        stream_name -- data stream name in Amazon Kinesis to which data is being sent
    """
    # invoke url for one record
    # invoke_url = "https://<APIInvokeURL>/<DeploymentStage>/streams/<stream_name>/record"
    
    invoke_url='https://hh632okm50.execute-api.us-east-1.amazonaws.com/prod/streams/'
    headers = {'Content-Type': 'application/json'}
    
    data = json.dumps({
        "StreamName": stream_name,
        "Data": data,
        "PartitionKey": "partition-1"
    }, default=lambda d: d.isoformat())  # convert datetime to serializable isoformat

    response = requests.request("PUT", invoke_url + stream_name + "/record", headers=headers, data=data)

    if response.status_code != 200:
        print(f"{response.status_code} : {response.content}")
    else:
        print(response.content)


def send_one_post():
    """Fetch data from the 3 tables on the RDS instance and
    send it to the corresponding Data streams on Kinesis
    """
     # Data streams created in Kinesis:
    streams = ['streaming-12e371d757c1-pin', 'streaming-12e371d757c1-geo', 'streaming-12e371d757c1-user']

    for item in zip(fetch_post_data(), streams):
        send_data(item[0], item[1])


def run_infinite_post_data_loop():
    """Simulate sending posts on Pinterest
    """
    while True:
        sleep(random.randrange(0, 2))
        send_one_post()


if __name__ == "__main__":
    print('Working...')
    # run_infinite_post_data_loop()
    for _ in range(10):
        send_one_post()
