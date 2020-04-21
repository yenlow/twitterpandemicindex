import pandas as pd
import pickle
from lda.utils_lda import *
import pyLDAvis
pyLDAvis.enable_notebook()

import matplotlib.pyplot as plt
% matplotlib inline

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 2000)
pd.set_option('display.width', 2000)

output_folder = '20200421_191436'

#load model
ldaseq = LdaSeqModel.load(f'output/{output_folder}/ldaseq.pkl')

#load gensim_df, vocab and dictionary
with open(f'output/{output_folder}/gensim_inputs.pkl', 'rb') as f:
  df = pickle.load(f)
  gensim_df = pickle.load(f)
  id2word = pickle.load(f)
  dictionary = pickle.load(f)

# View number of time_slices
# Label dates and topics
ldaseq.num_time_slices
date_labels = ['03/05_Thu','03/06_Fri','03/07_Sat','03/08_Sun','03/09_Mon']
topic_labels = ['International','Domestic','Things']

topic_num=1
t = 0 #timeslice

#Topic evolution
term_evol={}
counter = 0
for timeslice in ldaseq.print_topic_times(topic_num,20):
  term_evol[date_labels[counter]] = [t for t, f in timeslice]
  counter += 1
print(pd.DataFrame(term_evol))

#Topic evolution
# pyLDAvis req notebook
doc_topic, topic_term, doc_lengths, term_frequency, vocab = ldaseq.dtm_vis(time=t, corpus=gensim_df)
vis_wrapper = pyLDAvis.prepare(topic_term_dists=topic_term, doc_topic_dists=doc_topic, doc_lengths=doc_lengths, vocab=vocab, term_frequency=term_frequency)
pyLDAvis.display(vis_wrapper)

# Get top tweets
#topic 1 vstacked on top of topic 2 etc
top_docs_df = top_docs(doc_topic, df, topn=100)
print(top_docs_df[0:20])

# Topic proportion over time
doc_topic_by_time = doc_topic_dist(ldaseq)
topic_freq_by_time = pd.DataFrame([x.mean(axis=0) for x in doc_topic_by_time],
                                  index=date_labels,
                                  columns=topic_labels)
print(topic_freq_by_time)

# Plot topic proportion over time
ax = topic_freq_by_time.plot.area(title='Daily topic proportion', legend='reverse', rot=25)
ax.set_xticks(range(ldaseq.num_time_slices))
ax.set_xticklabels(date_labels)
plt.show()

# Tweet volume * topic proportion over time
vol_prop_df = topic_freq_by_time.mul(pd.Series(ldaseq.time_slice, index=date_labels),axis=0)
print(vol_prop_df)

# Plot tweet volume * topic proportion over time
ax = vol_prop_df.plot.area(title='Daily number of tweets by topic', legend='reverse', rot=25)
ax.set_xticks(range(ldaseq.num_time_slices))
ax.set_xticklabels(date_labels)
plt.show()
