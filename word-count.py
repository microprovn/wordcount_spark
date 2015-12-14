import re
from pyspark import SparkConf , SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)

#lower the text and split by regular expression
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile("file:///data/book.txt")
words = input.flatMap(normalizeWords)

#wordCounts = words.countByValue()

#sort by word
wordCounts = words.map(lambda x : (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()

results = wordCountsSorted.collect()

#for word , count in wordCounts.items():
for result in results:
    count = str(result[0])
    word = result[1].encode("ascii","ignore")
    if (word):
        print word,count
