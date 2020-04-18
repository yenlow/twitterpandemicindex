# Collect tweets via Twitter Streaming API onto AWS Dynamo

from urllib3.exceptions import ProtocolError
from api.awsAPI import aws_instance
from api.twitterAPI import StreamListener2Dynamo

def main():
    keywords_list = ['virus', 'covid', 'coronavirus']

    # Connect to AWS API and create a boto3.Session
    awsObi = aws_instance()
    awsObi.create_session()
    # Set dynamodb table for saving tweets
    table = awsObi.set_dynamodb_table('tweets_covid')

    # Connect to Twitter streaming API
    streamListener = StreamListener2Dynamo(table=table) #initialize API endpoint
    streamListener.login() #auth
    streamListener.create_stream()
#    status = streamListener.api.get_status(1245534920643997699)
#    data = status._json
    while True:
        try:
            streamListener.stream.filter(track=keywords_list, languages=["en"], stall_warnings=True)
        except (ProtocolError, AttributeError):
            continue


if __name__ == '__main__':
    main()