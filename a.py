import sys
import collections
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
import warc
conf = SparkConf()
conf.setAppName("AzimTest1")

sc = SparkContext(conf=conf)

KEYNAME = "WARC-TREC-ID"
INFILE = sys.argv[1]
OUTFILE = sys.argv[2]

rdd = sc.newAPIHadoopFile(INFILE,
    "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text",
    conf={"textinputformat.record.delimiter": "WARC/1.0"})
def find_google(record):
    # finds google
    _, payload = record
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    if key and ('http' in payload):
        yield key + '\t' + 'http' + '\t' + '/m/045c7b'


#rdd.saveAsTextFile(OUTFILE)


rdd = rdd.flatMap(find_google)

rdd = rdd.saveAsTextFile(OUTFILE)
