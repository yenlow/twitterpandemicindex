import pickle, glob
import pandas as pd

from pyspark.sql.functions import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 2000)
pd.set_option('display.width', 2000)

######### Read in real-time tweets from Streaming API (300K)
df = spark.read.csv('data/tweets_*.txt', sep="\t",header=True)
df.printSchema()
df.show(20,False)
df.count()

#filter away tweets with no id (small % saved in wrong format)
df.where(~col('id').rlike('\\d+')).show(100,False)
df = df.where(col('id').rlike('\\d+'))
df.count()

(df
.withColumn('created',to_timestamp(col('created_ts')))
.select(min('created'),max('created')
).collect()

# Small df saved to pd_df instead
df_pd = df.toPandas()
df_pd.to_csv('data/df_stream.txt',sep="\t", encoding='utf-8')



######### Read in past tweets from Search API (only 200)
pickles = glob.glob("data/tweets*.pkl")

tweets = []
for p in pickles:
    tweets += pickle.load(open(p, 'rb'))

# Get the selected fields
tweets_sel = []
for tweet in tweets:
    tweets_sel.append([
                        tweet.id
                        ,tweet.full_text
                        ,tweet.created_at
                        ,tweet.user.id
                        ,tweet.user.name
                        ,tweet.user.screen_name
                        ,tweet.user.location
                        ,tweet.user.statuses_count
                        ,tweet.user.verified
                        ,tweet.favorite_count
                        ,tweet.favorited
                        ,tweet.retweet_count
                        ,tweet.entities
                        #,tweet.entities['hashtags']
                        #,tweet.entities['user_mentions']['screen_name']
                    ])

colnames = [
            'id'
            ,'text'
            ,'created_ts'
            ,'user_id'
            ,'user_name'
            ,'user_screen_name'
            ,'user_location'
            ,'user_statuses_count'
            ,'user_verified'
            ,'favorite_count'
            ,'favorited'
            ,'retweet_count'
            ,'entities'
            ]

df_tweets = pd.DataFrame(data=tweets_sel, columns=colnames)
df_tweets = df_tweets.set_index('id')
df_tweets.shape

#View duplicated tweets
df_tweets.loc[df_tweets.index.duplicated(keep=False)].sort_index()
#Keep only first of duplicate
df_tweets = df_tweets.loc[df_tweets.index.duplicated(keep='first')].sort_index()
df_tweets.shape

#df_tweets = pd.read_csv('data/df_tweets.csv',sep="\t")
df_tweets.to_csv('data/df_tweets.csv',sep="\t")