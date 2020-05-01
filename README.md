# About
Just like the stock index, the pandemindex reflects global sentiment about the COVID-19 pandemic.<br>
The pandemindex is calculated from 200 million tweets about the COVID-19 pandemic. <br>
Slide along the pandemindex and see the most frequent words, hashtags, emojis and symptoms used in the tweets of that day. 

As examples, the pandemindex follows the ups and downs of several milestone events ([CNN timeline](https://www.cnn.com/2020/02/06/health/wuhan-coronavirus-timeline-fast-facts/index.html)):
* 2020-02-02: first death outside China in the Phillipines
* 2020-02-07: the world mourns the death of Dr Li Wenliang whose early warning about the coronavirus was silenced by China
* 2020-02-29: first death in the US (Washington state)
* 2020-03-03: Federal Reserve drops interest rate by 0.5%
* 2020-03-11: WHO declares COVID-19 a pandemic
* 2020-03-13: US announces relief package

# Architecture


# What this repo does
1. Main source of tweets is the Large Scale COVID-19 Twitter Chatter dataset for Open Scientific Research by [Banda et al](https://zenodo.org/record/3757272#.XqxskRNKh24)
2. Tweets can also be collected via Twitter Streaming API or Search API into .txt (`text_preprocess/tweetstream_txt.py`)
3. Reads tweets and metadata into pyspark or pandas dataframes (`text_preprocess/readtweets.py`)
4. Preprocess tweet text (extended up to 280 char): tokenize, remove stop words and duplicates, normalize terms, 
 hashtags, mentions, emoticons (`text_preprocess/utils_preprocess.py`)
5. Annotate tweets with symptoms, places (see [repo](https://github.com/thepanacealab/covid19_biohackathon))
6. `/data_preprocess` reads in datasets from covid19.ascend.io (`read_ascend.py`) or Google BigQuery (`read_bigquery.py`)
7. Topic modeling with Dynamic LDA (`lda/fit_lda.py`)
8. Interpret and visualize dynamic lda (`lda/interpret_ldaseq.py`)

## Amend config_copy.py
Fill in with your credentials and rename file to config.py