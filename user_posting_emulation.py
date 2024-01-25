from abc import ABC, abstractmethod, abstractproperty
import random
from time import sleep
from database_utils import AWSDBConnector
import json
import requests

random.seed(100)

aws_dbconnector = AWSDBConnector("db_creds.yaml")

class PostingEmulation(ABC):

    def __init__(self, destination_list, headers, method, url):
        self.destination_list = destination_list
        self.headers = headers
        self.method = method
        self.invoke_url_template = url

    @abstractmethod
    def prepare_payload(self):
        pass

    def send_data(self, data, destination):
        payload = self.prepare_payload(data, destination)

        url = self.invoke_url_template.format(destination=destination)

        response = requests.request(self.method, url, headers=self.headers, data=payload)
        if response.status_code != 200:
            print(f"{response.status_code} : {response}")
        else:
            print(f"Data sent to {destination}")

    def send_one_post(self):
        """Fetch data from the 3 tables on the RDS instance and
        send it to the corresponding Data streams on Kinesis or topics on the MSK
        """
        for data, destination in zip(aws_dbconnector.fetch_post_data(), self.destination_list):
            self.send_data(data, destination)


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
    
    # Data destination - Kafka topics in MSK
    KAFKA_TOPICS = ['12e371d757c1.pin', '12e371d757c1.geo', '12e371d757c1.user']

    def __init__(self):
        self.destination_list = self.KAFKA_TOPICS
        self.headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        self.method = "POST"
        self.invoke_url_template = r"https://hh632okm50.execute-api.us-east-1.amazonaws.com/prod/topics/{destination}/"

    def prepare_payload(self, data:dict, topic):
        return json.dumps({
            "records": [{ "value": data }]
        }, default=lambda d: d.isoformat())  # convert datetime to serializable isoformat


class StreamEmulation(PostingEmulation):

    # Data destination - data streams in Kinesis
    DATA_STREAMS = ['streaming-12e371d757c1-pin', 'streaming-12e371d757c1-geo', 'streaming-12e371d757c1-user']
    
    def __init__(self):
        self.destination_list = self.DATA_STREAMS
        self.headers = {'Content-Type': 'application/json'}
        self.method = "PUT"
        self.invoke_url_template = r"https://hh632okm50.execute-api.us-east-1.amazonaws.com/prod/streams/{destination}/record"

    def prepare_payload(self, data, stream_name):
        return json.dumps({
            "StreamName": stream_name,
            "Data": data,
            "PartitionKey": "partition-1"
        }, default=lambda d: d.isoformat())  # convert datetime to serializable isoformat


if __name__ == "__main__":
    b = BatchEmulation()
    b.send_posts(1)

    s = StreamEmulation()
    s.send_posts(1)