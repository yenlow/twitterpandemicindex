import re, json
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer
from wordcloud import WordCloud
import matplotlib.pyplot as plt

try:
    stopwords_Eng = stopwords.words('english')
    lemma = WordNetLemmatizer()
    porter = PorterStemmer()
#    nltk.data.find('corpora/stopwords')
except LookupError:
    import nltk
    nltk.download('stopwords')
    nltk.download('wordnet')

stop = set(stopwords_Eng +
           ['rt', 'amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
            'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via', 'dont',
            'one','com','make','say','would', 'im',
            'should','could','really','way', 'ya','also',
            'while','know','free','always','put',
            'week','went','wasn','was','used','ugh','try','kind', 'http','much',
            'next','app','using','&amp',"w/", "ued",
            'btw','imo','may','says','said',
            'see','keep','please','still','look','possible','taking',
            'already','must','want','every','yet','since','told','likely','another',
            'aint','apparent','like','think','due','take','says','getting','tell',
            'even','ever','actually','absolutely','about','any','am','need',
            'go','going','retweet',
            'virus','covid','convid','coronavid','corona','coronavirus','covidー',
            'prep','ready'
            ])


# Functions using the above regex to silence or locate emoticons, hashtags
def rm_nonwords(s):
    # strings to remove
    regex_rm = [
        #    emoticons_str,
        r'<[^>]+>',  # HTML tags
        #    r'(?:@[\w_]+)', # @-mentions
        #    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
        r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',  # URLs
        r'(?:(?:\d+,?)+(?:\.?\d+)?)',  # numbers
        #    r'[^\w\s]'  # not alphanumeric or whitespace
        #    r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
        #    r'(?:[\w_]+)', # other words
    ]
    str_gsub = re.compile(r'(' + '|'.join(regex_rm) + ')', re.VERBOSE | re.IGNORECASE)
    return str_gsub.sub("",s)

def get_emoticons(s):
    # Regex to recognize nonwords
    emoticons_str = r"""
        (?:
            [:=;] # Eyes
            [oO\-]? # Nose (optional)
            [D\)\]\(\]/\\OpP]{0,1} # Mouth
        )"""
    emoticon_re = re.compile(emoticons_str, re.VERBOSE)
    #remember to exclude http:// from emoticons
    emoticons = emoticon_re.findall(re.sub("http[s]?://","",s))
    return emoticons

def get_hashtags(s):
    hash_re = re.compile("(?:^|\s)(\#[\w]+)")  # must start with #, not in midstring
    hashtags = hash_re.findall(s.lower())
    return hashtags

def parse_hashtags(s):
    # s = df.loc['1236843710857408512'].entities
    try:
        m = re.search("u'hashtags': (.*?), u'urls':", s)
        if m:
            s = m.group(1)
            s = s.replace('u\'', '\'')
            s = s.replace('\'', '\"')
            json_dict = json.loads(s)
            ans = list({i['text'] for i in json_dict})
            if len(ans)>0:
                return ans
            else:
                return ['']
    except:
        return ['']

# removing stemming for now, probably unnecessary
def stem_lemmatize(token):
    if len(token) < 3:
        return token
    token=lemma.lemmatize(token)
    token=porter.stem(token)
    return token

def normalize_terms(s):
#    s = re.sub(r'^virus$|covid|convid|coronavid|corona','coronavirus', s)
    s = re.sub(r'corona','', s)
    s = re.sub(r'prepare|preparation|prepared|preparedness|prepd','prep', s)
    s = re.sub(r'testing|tests|tested', 'test', s)
    s = re.sub(r'cases','case', s)
    s = re.sub(r'realdonaldtrump','trump', s)
    s = re.sub(r'hospitals','hospital', s)
    s = re.sub(r'^cent$|centers|central|centre|centres', 'center', s)
#    s = re.sub(r'^cdc[\S]+', 'cdc', s)
    return s

def preprocessor(s):
    s = rm_nonwords(s)
    s = normalize_terms(s)
    return s

def wordcloud_fig(text,max_words=100,outfig=None):
    if isinstance(text,dict):
        wc = WordCloud(width=800, height=800, max_words=max_words,
                  background_color='white',
                  min_font_size=10).generate_from_frequencies(text)
    elif isinstance(text,str):
        wc = WordCloud(width=800, height=800, max_words=max_words,
                  background_color='white',
                  min_font_size=10).generate(text)
    plt.imshow(wc, interpolation='bilinear')
    plt.axis("off")
    plt.show()
    if outfig:
        plt.savefig(outfig)