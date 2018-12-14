#!/usr/bin/env bash

SCRIPT=${1:-"cluster.py"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTFILE=${3:-"sample"}
echo "Script is: " $SCRIPT
echo "Input is: " $INFILE
hadoop fs -rm -r $OUTFILE
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
# This assumes there is a python virtual environment in the "venv" directory
PYSPARK_PYTHON=$(readlink -f $(which python)) /home/bbkruit/spark-2.1.2-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/nltk_env/bin/python \
--conf spark.yarn.appMasterEnv.NLTK_DATA=./  \
--master yarn \
--deploy-mode cluster \
--num-executors 8 \
--executor-memory 2G \
--archives ~/anaconda3/envs/nltk_env.zip#VENV,tokenizers.zip#tokenizers,taggers.zip#taggers,1.zip#STANFORD \
--files stanford-ner.jar#JARFILE \
$SCRIPT $INFILE $OUTFILE $ES_NODE:$ES_PORT

hdfs dfs -cat $OUTFILE"/*" > "result"
ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
sleep 15
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"
INFILE=${4:-"result"}
OUTFILE=${5:-"finlal"}
FINAL_RESULT=${6:-"result.tsv"}
#ANNOTATIONS=${4:-"data/sample.annotations.tsv"}
python3 mention2entity.py $INFILE $OUTFILE $ES_NODE:$ES_PORT
python3 transformer.py $OUTFILE $FINAL_RESULT
#python3 score.py $ANNOTATIONS $FINAL_RESULT
kill $ES_PID
