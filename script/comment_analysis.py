import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from functools import reduce
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession, functions, types
import re
import string

# cluster_seeds = ['cassandra']
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('Yelp Data Analysis').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

def main(keyspace):
    reviews_rdd = data.select("verified_reviews").rdd.flatMap(lambda x: x)
    header = reviews_rdd.first()
    data_rmv_col = reviews_rdd.filter(lambda row: row != header)
    lowerCase_sentRDD = data_rmv_col.map(lambda x : x.lower())
    sentenceTokenizeRDD = lowerCase_sentRDD.map(sent_TokenizeFunct)
    # number_removed = sentenceTokenizeRDD.map(remove_number)
    wordTokenizeRDD = sentenceTokenizeRDD.map(word_TokenizeFunct)
    stopwordRDD = wordTokenizeRDD.map(removeStopWordsFunct)
    rmvPunctRDD = stopwordRDD.map(removePunctuationsFunct)
    lem_wordsRDD = rmvPunctRDD.map(lemmatizationFunct)
    joinedTokens = lem_wordsRDD.map(joinTokensFunct)
    extractphraseRDD = joinedTokens.map(extractPhraseFunct)
    sentimentRDD = extractphraseRDD.map(sentimentWordsFunct)
    sentimentRDD.collect()

def sent_TokenizeFunct(x):
    return nltk.sent_tokenize(x)


def word_TokenizeFunct(x):
    splitted = [word for line in x for word in line.split()]
    return splitted

def removeStopWordsFunct(x):
    from nltk.corpus import stopwords
    stop_words=set(stopwords.words('english'))
    filteredSentence = [w for w in x if not w in stop_words]
    return filteredSentence


def removePunctuationsFunct(x):
    list_punct=list(string.punctuation)
    filtered = [''.join(c for c in s if c not in list_punct) for s in x] 
    filtered_space = [s for s in filtered if s] #remove empty space 
    return filtered

def lemmatizationFunct(x):
    nltk.download('wordnet')
    lemmatizer = WordNetLemmatizer()
    finalLem = [lemmatizer.lemmatize(s) for s in x]
    return finalLem

def joinTokensFunct(x):
    joinedTokens_list = []
    x = " ".join(x)
    return x

def extractPhraseFunct(x):
    from nltk.corpus import stopwords
    stop_words=set(stopwords.words('english'))
    def leaves(tree):
        """Finds NP (nounphrase) leaf nodes of a chunk tree."""
        for subtree in tree.subtrees(filter = lambda t: t.label()=='NP'):
            yield subtree.leaves()
    
    def get_terms(tree):
        for leaf in leaves(tree):
            term = [w for w,t in leaf if not w in stop_words]
            yield term
    sentence_re = r'(?:(?:[A-Z])(?:.[A-Z])+.?)|(?:\w+(?:-\w+)*)|(?:\$?\d+(?:.\d+)?%?)|(?:...|)(?:[][.,;"\'?():-_`])'
    grammar = r"""
    NBAR:
        {<NN.*|JJ>*<NN.*>}  # Nouns and Adjectives, terminated with Nouns
        
    NP:
        {<NBAR>}
        {<NBAR><IN><NBAR>}  # Above, connected with in/of/etc...
    """
    chunker = nltk.RegexpParser(grammar)
    tokens = nltk.regexp_tokenize(x,sentence_re)
    postoks = nltk.tag.pos_tag(tokens) #Part of speech tagging 
    tree = chunker.parse(postoks) #chunking
    terms = get_terms(tree)
    temp_phrases = []
    for term in terms:
        if len(term):
            temp_phrases.append(' '.join(term))
    
    finalPhrase = [w for w in temp_phrases if w] #remove empty lists
    return finalPhrase


def sentimentWordsFunct(x):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer() 
    senti_list_temp = []
    total = 0
    for i in x:
        y = ''.join(i) 
        vs = analyzer.polarity_scores(y)
        total = total+float(vs['compound'])
    sentiment_list  = []
    if (total>0):
        sentiment_list.append((x, "Positive"))
    elif (total<0):
        sentiment_list.append((x, "Negative"))
    else:
        sentiment_list.append((x, "Neutral"))
    return sentiment_list

if __name__ == "__main__":
    keyspace = sys.argv[1]
    main(keyspace)

