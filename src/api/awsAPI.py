from api.config import aws_api
import boto3

# setup dynamodb table
class aws_instance():
    def __init__(self):
        self.session = None

    def _login(self):
        aws_region, aws_access_key_id, aws_access_secret = aws_api()
        return aws_region, aws_access_key_id, aws_access_secret

    def create_session(self):
        aws_region, aws_access_key_id, aws_access_secret = self._login()
        session = boto3.Session(region_name=aws_region,
                                aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_access_secret)
        self.session = session

    def set_dynamodb_table(self, table):
        return self.session.resource('dynamodb').Table(table)
