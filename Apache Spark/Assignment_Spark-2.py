
# coding: utf-8

# In[3]:


from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

import re
import sys
import math

def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

def ngram(data,n_gram=2,sep='_'):
    l = []
    for i in range(len(data) - n_gram +1):
        l.append(sep.join(data[i:i+n_gram]))
    return l

def filter_stopwords(data):
    l = []
    for i in range(len(data)):
        if data[i].lower() not in stop_words:
            l.append(data[i].lower())
    return l

def compute_npmi(x):
    Pa = word_stats_b.value[x[0][0]]
    Pb = word_stats_b.value[x[0][1]]
    PMI = math.log(x[1]/(Pa * Pb))
    NPMI = float(PMI) / math.log(x[1]) * -1
    return NPMI

def fix_stopword(w):
    return w.strip()
   
    
stop_words = sc.textFile("/datasets/stop_words_en.txt")     .map(fix_stopword)     .collect()
stop_words = set(stop_words)      

wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16)     .map(parse_article)     .cache()


filtered_words = wiki.map(filter_stopwords)
word_stats = filtered_words.flatMap(lambda x: (x))     .map(lambda s: (s.lower(), 1))     .reduceByKey(lambda x,y:x+y)
word_count = word_stats.count()
word_stats = word_stats.filter(lambda x: (x[1] > 500))     .map(lambda x: (x[0], float(x[1])/word_count)) 
word_stats_b = sc.broadcast(word_stats.collectAsMap())

rdd1 = filtered_words.flatMap(ngram)     .map(lambda s: (s.lower(), 1))     .reduceByKey(lambda x,y:x+y) 
bigram_count = rdd1.count()
bigrams = rdd1.filter(lambda x: (x[1] > 500))     .map(lambda x: ((x[0].split('_')), float(x[1])/bigram_count))     .map(lambda x: (compute_npmi(x), x[0][0], x[0][1])) 

x = bigrams.takeOrdered(39, key = lambda x: -x[0])
for xx in x:
    print ("%s_%s" % (xx[1], xx[2]))

