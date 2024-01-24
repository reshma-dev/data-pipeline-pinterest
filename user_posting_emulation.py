from abc import ABC, abstractmethod, abstractproperty
import random
from time import sleep
from database_utils import AWSDBConnector
import json
import requests

random.seed(100)

aws_dbconnector = AWSDBConnector("db_creds.yaml")

class PostingEmulation(ABC):

    def __init__(self, destination_list):
        self.destination_list = destination_list

    @abstractmethod
    def send_data(self):
        pass

    def send_one_post(self):
        """Fetch data from the 3 tables on the RDS instance and
        send it to the corresponding Data streams on Kinesis or topics on the MSK
        """
        for item in zip(aws_dbconnector.fetch_post_data(), self.destination_list):
            self.send_data(item[0], item[1])


    def run_infinite_post_data_loop(self):
        """Simulate sending posts on Pinterest
        """
        while True:
            sleep(random.randrange(0, 2))
            self.send_one_post()


    def send_posts(self, count):
        """Simulate sending `count` posts on Pinterest
        """
        for _ in range(count):
            sleep(random.randrange(0, 2))
            self.send_one_post()


class BatchEmulation(PostingEmulation):

    def __init__(self):
        # Data destination - Kafka topics in MSK
        self.destination_list = ['12e371d757c1.pin', '12e371d757c1.geo', '12e371d757c1.user']


    def send_data(self, data:dict, topic:str):
        """Send data to kafka topic

        Arguments:
            data -- topic's data as dictionary
            topic -- name of topic to send data to
        """
        payload = json.dumps({
            "records": [{ "value": data }]
        }, default=lambda d: d.isoformat())  # convert datetime to serializable isoformat

        # Invoke URL for the API deployed in API Gateway to send data to MSK cluster 
        invoke_url = r"https://hh632okm50.execute-api.us-east-1.amazonaws.com/prod/topics/{topic}/"
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

        url = invoke_url.format(topic=topic)
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code != 200:
            print(f"{response.status_code} : {response}")
        else:
            print(f"Data sent to topic {topic}")


class StreamEmulation(PostingEmulation):

    def __init__(self):
        # Data destination - data streams in Kinesis
        self.destination_list = ['streaming-12e371d757c1-pin', 'streaming-12e371d757c1-geo', 'streaming-12e371d757c1-user']
        
    def send_data(self, data:dict, stream_name:str):
        """Send data to stream

        Arguments:
            data -- stream's data as dictionary
            stream_name -- data stream name in Amazon Kinesis to which data is being sent
        """
        # invoke url format for one record
        # "https://<APIInvokeURL>/<DeploymentStage>/streams/<stream_name>/record"
        
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
            print(f"Data sent to stream {stream_name} - {response.content}")

if __name__ == "__main__":
    b = BatchEmulation()
    b.send_posts(1)

    s = StreamEmulation()
    s.send_posts(1)