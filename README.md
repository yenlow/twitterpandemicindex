# About
lumiatacovid19 hackathon

# What this repo does (so far)
1. Collect tweets via Twitter Streaming API or Search API into .txt (`text_preprocess/tweetstream_txt.py`)
2. Reads tweets and metadata into pyspark or pandas dataframes (`text_preprocess/readtweets.py`)
3. Preprocess tweet text (extended up to 280 char): tokenize, remove stop words and duplicates, normalize terms, 
 hashtags, mentions, emoticons (`text_preprocess/utils_preprocess.py`)
4. Annotate tweets with symptoms, places (see [repo](https://github.com/thepanacealab/covid19_biohackathon))
5. Topic modeling with Dynamic LDA (`lda/fit_lda.py`)
6. Viz and check dynamic lda (to be added)
7. `/data_preprocess` reads in datasets from covid19.ascend.io (`read_ascend.py`) or Google BigQuery (`read_bigquery.py`)

## Amend config_copy.py
Fill in with your credentials and rename file to config.py