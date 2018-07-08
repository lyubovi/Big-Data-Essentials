# Working with Apache Spark in Python

The following [docker container](https://hub.docker.com/r/bigdatateam/spark-course1/) 
was used for the assignments
```
docker pull bigdatateam/spark-course1
```

Run the container
```
docker run -it --rm -p 8888:8888 bigdatateam/spark-course1
```

To get bash shell for the container:
```
docker ps
docker exec -it [process id] /bin/bash
```
In this assignment you will use Spark to compute various statistics for word pairs. At the same time, you will learn some simple techniques of natural language processing.

Dataset location: /data/wiki/en_articles
```
Format: article_id <tab> article_text
```
While parsing the articles, do not forget about Unicode (even though this is 
an English Wikipedia dump, there are many characters from other languages), 
remove punctuation marks and transform words to lowercase to get the correct 
quantities. Here is a starting snippet:
```
#! /usr/bin/env python

from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local[2]"))

import re

def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
    except ValueError as e:
        return []
    text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
    words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
    return words

wiki = sc.textFile("/data/wiki/en_articles_part1/articles-part", 16) \
         .map(parse_article)
result = wiki.take(1)[0]

for word in result[:50]:
    print word
```    