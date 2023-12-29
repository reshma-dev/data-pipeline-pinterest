import sqlalchemy

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