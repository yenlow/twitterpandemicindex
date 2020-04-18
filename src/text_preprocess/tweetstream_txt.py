# Collect tweets via Twitter Streaming API into a text file

import logging, io
import pandas as pd
from api.twitterAPI import StreamListener

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 2000)
pd.set_option('display.width', 2000)

logger = logging.getLogger()

###### Using Twitter Streaming API (real-time/future)
def main(keywords_list, f_out):
    streamListener = StreamListener(f_out) #initialize API endpoint
    streamListener.login() #auth
    streamListener.create_stream()
    with io.open(streamListener.f_out, "w", encoding='UTF-8') as f:
        f.write(u"id\tcreated_ts\ttext\tuser.name\tlocation\tverified\tentities\tRT\tquoted_text\n")
    streamListener.stream.filter(   track=keywords_list,
    # location is an OR param, not AND
    #                               locations=[-125.0011, 24.9493, -66.9326, 49.5904],
                                    languages=["en"])

if __name__ == "__main__":
    keywords_list =["virus prep", "virus prepare", "virus preparation",
                    "covid prep", "covid prepare", "covid preparation",
                    "coronavirus prep", "coronavirus prepare", "coronavirus preparation",
                    "virus ready", "covid ready", "coronavirus ready",
                    "virus readiness", "covid readiness", "coronavirus readiness"]
    main(keywords_list, f_out="tweets_03091145.txt")