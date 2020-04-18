from api.config import twitter_api
import sys, logging, io
import tweepy as tw
from multiprocessing import Queue
from threading import Thread

###### Using Twitter Streaming API (real-time/future)

# Save tweets via json to AWS dynamodb
#https://nicovibert.com/2019/07/30/twitter-apis-python-dynamodb/
#https://github.com/tweepy/tweepy/issues/237 use queues
class StreamListener2Dynamo(tw.StreamListener):
    """ A listener that continuously receives tweets and stores them in a
        DynamoDB database.
    """
    def __init__(self, table, nthreads=2):
        super().__init__()
        self.api = None
        self.table = table
        self.q = Queue()
        for i in range(nthreads):
            t = Thread(target=self.do_stuff)
            t.daemon = True
            t.start()

    def login(self):
        #get Twitter API credentials
        consumer_key, consumer_secret, access_token, access_secret = twitter_api()
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        logger = logging.getLogger()
        logger.info("API created")
        try:
            api.verify_credentials()
        except Exception as e:
            logger.error("Error creating API", exc_info=True)
            raise e
        self.api = api

    def create_stream(self):
        self.stream = tw.Stream(auth=self.api.auth, listener=self, tweet_mode='extended')

    def do_stuff(self):
        while True:
            self.q.get()
            self.q.task_done()

    def on_status(self, status):
        data = status._json

        content = {}
        content['id'] = data['id']
        content['created_at_utc'] = data['created_at']
#        content['created_at_utc'] = int(data['timestamp_ms'])

        # get extended tweet of 280 char
        if hasattr(status, "extended_tweet"):
            content['text'] = status.extended_tweet["full_text"]
        else:
            content['text'] = status.text

        content['retweet'] = int(hasattr(status, "retweeted_status"))
        content['retweet_count'] = data['retweet_count']

        #parse user info
        content['user_id'] = data['user']['id']
        content['screen_name'] = data['user']['screen_name']
        content['utc_offset'] = data['user']['utc_offset']
        content['time_zone'] = data['user']['time_zone']
        content['place'] = data['place']
        content['coordinates'] = data['coordinates']

        #parse entities
        content['hashtags'] = [
            x['text'] for x in data['entities']['hashtags'] if x['text']]
        content['user_mentions'] = [
            x['screen_name'] for x in data['entities']['user_mentions'] if x['screen_name']]
        content['urls'] = [x['url'] for x in data['entities']['urls'] if x['url']]

        # check if this is a quote tweet
        is_quote = hasattr(status, "quoted_status")
        if is_quote:
            # check if quoted tweet's text has been truncated before recording it
            if hasattr(status.quoted_status, "extended_tweet"):
                content['quoted_text'] = status.quoted_status.extended_tweet["full_text"]
            else:
                content['quoted_text'] = status.quoted_status.text

        print(content['text'] + '\n')

        try:
            self.table.put_item(Item=content)
        except Exception as e:
            print(str(e))

    def on_error(self, status_code):
        print('Encountered error with status code: {}'.format(status_code))
#        sys.exit()
        return True  # Don't kill the stream

    def on_timeout(self):
        print('Timeout...')
        return True  # Don't kill the stream



# Save tweets to .tsv
class StreamListener(tw.StreamListener):
    def __init__(self, f_out="out.tsv"):
        self.api = None
        self.f_out = f_out

    def login(self):
        #get Twitter API credentials
        consumer_key, consumer_secret, access_token, access_secret = twitter_api()
        auth = tw.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_secret)
        api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        try:
            api.verify_credentials()
        except Exception as e:
            logger.error("Error creating API", exc_info=True)
            raise e
        logger = logging.getLogger()
        logger.info("API created")
        self.api = api

    def on_status(self, status):
        if not status.place or status.place.country_code == 'US':

            id = status.id

            # if "retweeted_status" attribute exists, flag this tweet as a retweet.
            is_retweet = int(hasattr(status, "retweeted_status"))


            # check if text has been truncated
            if hasattr(status, "extended_tweet"):
                text = status.extended_tweet["full_text"]
            else:
                text = status.text

            # check if this is a quote tweet.
            is_quote = hasattr(status, "quoted_status")
            quoted_text = ""
            if is_quote:
                # check if quoted tweet's text has been truncated before recording it
                if hasattr(status.quoted_status, "extended_tweet"):
                    quoted_text = status.quoted_status.extended_tweet["full_text"]
                else:
                    quoted_text = status.quoted_status.text

            # remove characters that might cause problems with csv encoding
            remove_characters = ["\t", "\n"]
            for c in remove_characters:
                text = text.replace(c, " ")
                quoted_text = quoted_text.replace(c, " ")

#            print(u"{}\t{}".format(id, text.encode("utf-8")))

            with io.open(self.f_out, "a", encoding='utf-8') as f:
                f.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (
                id, status.created_at, text, status.user.name, status.user.location, status.user.verified, status.entities, is_retweet, quoted_text))

    def create_stream(self):
        self.stream = tw.Stream(auth=self.api.auth, listener=self, tweet_mode='extended')

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")
        sys.exit()
