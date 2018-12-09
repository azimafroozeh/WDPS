#!/usr/bin/env bash

hadoop fs -rm -r testazim1
rm result

SCRIPT=${1:-"test.py"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTFILE=${3:-"testazim1"}

# This assumes there is a python virtual environment in the "venv" directory
PYSPARK_PYTHON=$(readlink -f $(which python)) /home/bbkruit/spark-2.1.2-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/nltk_env/bin/python \
--master local[*] \
--archives ~/anaconda3/envs/nltk_env.zip#VENV \
$SCRIPT $INFILE $OUTFILE
hdfs dfs -cat $OUTFILE"/*" > "result"
