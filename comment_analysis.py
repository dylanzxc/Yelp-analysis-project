import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

import datetime
from functools import reduce
import nltk
#nltk.download('punkt')
#nltk.download('stopwords')
#nltk.download('vader_lexicon')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession, functions, types
import re
import string

# cluster_seeds = ['cassandra']
cluster_seeds = ['199.60.17.32', '199.60.17.65']
spark = SparkSession.builder.appName('comment_analysis').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
assert spark.version >= '2.4' # make sure we have Spark 2.4+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext


def main(keyspace):
# nltk.data.path.append('/home/zca92/nltk_data')
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table='review', keyspace=keyspace).load()
    reviews = data.select("review_id","text")
    reviews = reviews.withColumn('text', functions.lower(reviews['text']))
    null_remove = reviews.na.drop()
    sentenceTokenizedf = null_remove.select('review_id',sent_TokenizeFunct(null_remove['text']).alias('result'))
    wordTokenizedf = sentenceTokenizedf.select('review_id',word_TokenizeFunct(sentenceTokenizedf['result']).alias('result'))
    stopworddf = wordTokenizedf.select('review_id',removeStopWordsFunct(wordTokenizedf['result']).alias('result'))
    rmvPunctdf = stopworddf.select('review_id',removePunctuationsFunct(stopworddf['result']).alias('result'))
    lem_wordsdf = rmvPunctdf.select('review_id',lemmatizationFunct(rmvPunctdf['result']).alias('result'))
    joinedTokens = lem_wordsdf.select('review_id',joinTokensFunct(lem_wordsdf['result']).alias('result'))
    extractphrasedf = joinedTokens.select('review_id',extractPhraseFunct(joinedTokens['result']).alias('result'))
    sentimentdf = extractphrasedf.select('review_id',sentimentWordsFunct(extractphrasedf['result']).alias('result'))
    sentimentdf.write.format("org.apache.spark.sql.cassandra") \
    .options(table='comment_analysis_full', keyspace=keyspace).save()


@functions.udf(returnType = types.StringType())
def sent_TokenizeFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    return nltk.sent_tokenize(x)

@functions.udf(returnType = types.StringType())
def word_TokenizeFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    splitted = [word for line in x for word in line.split()]
    return splitted


@functions.udf(returnType = types.StringType())
def removeStopWordsFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    from nltk.corpus import stopwords
    stop_words=set(stopwords.words('english'))
    filteredSentence = [w for w in x if not w in stop_words]
    return filteredSentence
@functions.udf(returnType = types.StringType())
def removePunctuationsFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    list_punct=list(string.punctuation)
    filtered = [''.join(c for c in s if c not in list_punct) for s in x]
    filtered_space = [s for s in filtered if s]
    return filtered

@functions.udf(returnType = types.StringType())
def lemmatizationFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    #nltk.download('wordnet')
    lemmatizer = WordNetLemmatizer()
    finalLem = [lemmatizer.lemmatize(s) for s in x]
    return finalLem

@functions.udf(returnType = types.StringType())
def joinTokensFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    joinedTokens_list = []
    x = " ".join(x)
    return x

@functions.udf(returnType = types.StringType())
def extractPhraseFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
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

@functions.udf(returnType = types.StringType())
def sentimentWordsFunct(x):
    nltk.data.path.append('/home/zca92/nltk_data/')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    analyzer = SentimentIntensityAnalyzer()
    senti_list_temp = []
    total = 0
    for i in x:
        y = ''.join(i)
        vs = analyzer.polarity_scores(y)
        total = total+float(vs['compound'])
    if (total>0):
        result="Positive"
    elif (total<0):
        result="Negative"

    else:
        result="Neutral"

    return result

if __name__ == "__main__":
    keyspace = sys.argv[1]
    main(keyspace)
