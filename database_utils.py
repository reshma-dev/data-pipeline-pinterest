import sqlalchemy
from sqlalchemy import text
import random
import yaml

class AWSDBConnector:

    def __init__(self, creds_file_name):
        """ Initializes an instance of AWSDBConnector with database credentials loaded from a YAML file.

        Arguments:
            creds_file_name (str) -- The path to the YAML file containing database credentials.
        """

        with open(creds_file_name, 'r') as file:
            db_creds = yaml.safe_load(file)
        
        self.HOST = db_creds['HOST']
        self.USER = db_creds['USER']
        self.PASSWORD = db_creds['PASSWORD']
        self.DATABASE = db_creds['DATABASE']
        self.PORT = db_creds['PORT']


    def create_db_connector(self):
        """ Creates and returns a SQLAlchemy database engine based on the stored credentials

        Returns:
            sqlalchemy.engine.Engine -- A SQLAlchemy database engine
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
    

    def fetch_row(self, table_name:str, offset:int):
        """Fetch a row from the table at the given `offset`

        Arguments:    
            table_name -- str
                Name of the table to extract a data row from
        Returns:
            data as `dict`
        """
        sql = text(f"SELECT * FROM {table_name} LIMIT 1 OFFSET {offset}")
        
        engine = self.create_db_connector()
        with engine.connect() as connection:
            selected_rows = connection.execute(sql)
        
        return selected_rows.first()._mapping


    def fetch_post_data(self):
        """
        Fetch and return 1 row from all 3 tables
        pinterest_data    - contains data about posts being updated to Pinterest
        geolocation_data  - contains data about the geolocation of each Pinterest post found in pinterest_data
        user_data         - contains data about the user that has uploaded each post found in pinterest_data
        """
        random_row = random.randint(0, 11000)
        pin_result = dict(self.fetch_row(table_name="pinterest_data", offset=random_row))
        geo_result = dict(self.fetch_row(table_name="geolocation_data", offset=random_row))
        user_result = dict(self.fetch_row(table_name="user_data", offset=random_row))
        
        return pin_result, geo_result, user_result