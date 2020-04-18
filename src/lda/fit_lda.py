#!/usr/bin/env python
import pickle

from lda.functions_lda import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 2000)
pd.set_option('display.width', 2000)

seed = 0
ntopics = 3
alpha = 0.01
chain_variance = 0.005

if __name__ == '__main__':
	#Read in past tweets (via search API - only 200)
	# df_tweets = pd.read_csv('../data/df_tweets.txt', sep="\t")
	# df_tweets.shape

	# Read in past tweets (via streaming API)
	df_stream = pd.read_csv('../data/df_stream.txt', sep="\t", parse_dates=['created_ts'])
	df_stream.set_index('id', inplace=True)
	df_stream.drop(columns='Unnamed: 0', inplace=True)
	print(f"{df_stream.shape[0]} tweets of {df_stream.shape[1]} structured fields")

	df = get_data(df_stream, '2020-03-05', '2020-03-10', rm_duplicate=True)
	print(f"{df.shape[0]} tweets of {df.shape[1]} structured fields")

	# compute metadata
	docs_per_time = df['created_dt'].value_counts().sort_index()
	ndocs_per_time = docs_per_time.values.tolist()
	dates = [str(i) for i in docs_per_time.index]
	num_days = len(dates)
	print('docs_per_time')
	print(docs_per_time)

	# vectorize
	gensim_df, id2word, dictionary = vectorize_text(df)
	hashtag_mat, hashtag_voc = vectorize_hashtags(df)

	with open('../../data/gensim_inputs.pkl', 'wb') as f:
		pickle.dump(gensim_df, f)
		pickle.dump(id2word, f)
		pickle.dump(dictionary, f)
	f.close()

	freq_terms = sorted([(freq, id2word[id]) for id, freq in dictionary.dfs.items()],reverse=True)


	######### fit LDA
	lda = fit_LdaMulticore(gensim_df, id2word, ntopics, alpha,
							 passes=1, iterations=100)

	top_coherence = lda.top_topics(gensim_df)[0][1]
	lda.print_topics()
	lda.show_topic(2, topn=20)
#	lda.get_term_topics(810)  #blame trump
#	lda.get_term_topics(dictionary.token2id['case'])
#	sstats = lda.state.sstats.T


	###########fit dtm
	ldaseq = LdaSeqModel(corpus=gensim_df,
				time_slice=ndocs_per_time,
				num_topics=ntopics,
				passes=3, chunksize=100,
				initialize='ldamodel',
				lda_model=lda,
				id2word=id2word,
				alphas=alpha,
				chain_variance=chain_variance)

	ldaseq = fit_LdaSeqModel(  ntopics, gensim_df, id2word, dictionary,
							   ndocs_per_time, dates, passes=1,
							   alpha=alpha, chain_variance=chain_variance,
							   old_model=prev_fit, savetmp=True, metric='umass')





