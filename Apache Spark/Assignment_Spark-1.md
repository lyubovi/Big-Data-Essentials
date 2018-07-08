

```python
from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))

import re
import sys

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
    
    
wiki = sc.textFile("/data/wiki/en_articles_part/articles-part", 16) \
    .map(parse_article) \
    .cache()
rdd1 = wiki.flatMap(ngram) \
            .map(lambda s: (s, 1)) \
            .reduceByKey(lambda x,y:x+y) 
bigrams = rdd1.map(lambda x: ((x[0].split('_')), x[1]))  \
              .map(lambda x: (x[0][0], x[0][1], x[1]))
f = bigrams.filter(lambda x: (x[0].lower() == 'narodnaya')) \
    .map(lambda s: ("%s_%s\t%s" % (s[0].lower(), s[1].lower(), s[2])))

z = f.take(10)
for zz in z:
    print zz   


```

    narodnaya_gazeta	1
    narodnaya_volya	9

