# Download dailies and concatenate into big file and upload to dropbox

import pandas as pd
from api.ascendAPI import ascendClient
from utils.utils import *
from text_preprocess.utils_preprocess import *


A = ascendClient()
A.login()

# 1. get sentiments
df_sentiments = pd.read_csv('data/tweet_results/sentiment_vdr_rankings.csv',sep=",")
df_sentiments.rename(columns={'created_dt':'date',
                              'vdr_compound':'sentiment_score'},inplace=True)
df_sentiments.set_index('date',inplace=True,drop=True)


# 2. get emojis
df_emojis = pd.read_csv('data/tweet_results/emoji_use_daily.csv',sep=",")
df_emojis.rename(columns={'Date':'date',
                          'mentions':'tweet_volume'},inplace=True)
df_emojis_xtab = df_emojis.pivot(index='date',columns='emoji', values='tweet_volume')
top_emojis = df_emojis_xtab.max().sort_values(ascending=False)[0:100].index
df_emojis_xtab = df_emojis_xtab[top_emojis]


# 3. get hashtags
df_hashtags = pd.read_csv('data/tweet_results/Hashtag_rankings.csv',sep=",")
df_hashtags.rename(columns={'Date':'date',
                            'frequency':'tweet_volume'},inplace=True)
df_hashtags_xtab = df_hashtags.pivot(index='date',columns='hashtag', values='tweet_volume')

top_hashtags = df_hashtags_xtab.max().sort_values(ascending=False)[0:].index
top_hashtags = [i for i in top_hashtags if not re.search(r'covid|corona$|cor[a-z]+virus|coronaoutbreak|coronvirus|2019ncov|update',i)]
df_hashtags_xtab = df_hashtags_xtab[top_hashtags]


# 3. get symptoms
df_symptoms = pd.read_csv('data/tweet_results/symptom_counts_x_day.csv',sep=",")
df_symptoms.rename(columns={'symptom_text':'symptom',
                             'Frequency':'tweet_volume'},inplace=True)
df_symptoms = (df_symptoms
    .sort_values(['date','symptom','tweet_volume'],ascending=[True,True,False])
    .drop_duplicates(['date','symptom'], keep='first')
    )
df_symptoms_xtab = df_symptoms.pivot(index='date',columns='symptom', values='tweet_volume')
top_symptoms = df_symptoms_xtab.max().sort_values(ascending=False)[0:125].index
top_symptoms = [i for i in top_symptoms if re.search(r'[a-z]+',i)]
df_symptoms_xtab = df_symptoms_xtab[top_symptoms]

# 4. get dailies
df_dailies = pd.read_pickle('data/tweet_results/df_dailies.zip')
df_dailies_xtab = df_dailies.pivot(index='date',columns='term', values='tweet_volume')
df_dailies_xtab.index = df_dailies_xtab.index.astype(str)
top_terms = df_dailies_xtab.max().sort_values(ascending=False)[0:300].index
df_dailies_xtab = df_dailies_xtab[top_terms]
#df_dailies_xtab.reset_index().to_csv('/Users/yensia-low/Dropbox/transfer/dailies.tsv', sep="\t", index=False)


# 5. Read JHU cases table joined in ascend
df_cases = A.component2pd('yenlow_gmail_com','test','global_cases_deaths_recov')
df_cases.set_index('date',inplace=True,drop=True)
df_cases.rename(columns={'deaths':'death_cnt'},inplace=True)


# 5. Read global google mobility data from ascend
df_daily_mobility_tweet_vol = A.component2pd('yenlow_gmail_com','test','daily_mobility_global_tweet_vol')
df_daily_mobility_tweet_vol.set_index('date',inplace=True,drop=True)
#df_combined = df_daily_mobility_tweet_vol.join(df_sentiments,how='outer')
df_combined = pd.concat([df_cases, df_daily_mobility_tweet_vol,
                         df_sentiments],
                        join='outer',axis=1)

df_combined.to_json('data/global_daily_data.json', orient='table')
df_emojis_xtab.to_json('data/global_daily_emojis.json', orient='table')
df_hashtags_xtab.to_json('data/global_daily_hashtags.json', orient='table')
df_symptoms_xtab.to_json('data/global_daily_symptoms.json', orient='table')
df_dailies_xtab.to_json('data/global_daily_terms.json', orient='table')


# Generate wordcloud
dict_symptoms_freq = df_symptoms_xtab.max().sort_values(ascending=False).to_dict()
dict_hashtags_freq = df_hashtags_xtab.max().sort_values(ascending=False).to_dict()
dict_dailies_freq = df_dailies_xtab.max().sort_values(ascending=False).to_dict()
dict_emojis_freq = df_emojis_xtab.max().sort_values(ascending=False).to_dict()
# emoji_list = [(' '+k.encode('utf-8'))*int(v) for k,v in dict_emojis_freq.items()]
# emoji_text = '\n'.join(emoji_list)


wordcloud_fig(dict_symptoms_freq,100,'output/wc_symptoms.png')
wordcloud_fig(dict_hashtags_freq,100,'output/wc_hashtags.png')
wordcloud_fig(dict_dailies_freq,100,'output/wc_terms.png')
wordcloud_fig(dict_emojis_freq,100,None)