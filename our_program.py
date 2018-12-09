import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
conf = SparkConf()
conf.setAppName("AzimTest1")

sc = SparkContext(conf=conf)

data = sc.textFile('hdfs:///user/wdps1810/test.txt')
print(data.take(10))
#data1 = data.flatMap(lambda line: line + ("AzimTested"))
data.saveAsTextFile('hdfs:///user/wdps1810/testazim1')
