import requests
import json
import requests
from time import sleep
import random
import json
from database_utils import AWSDBConnector


random.seed(100)

aws_dbconnector = AWSDBConnector("db_creds.yaml")


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

    for item in zip(aws_dbconnector.fetch_post_data(), streams):
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
