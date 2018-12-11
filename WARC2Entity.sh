#!/usr/bin/env bash
ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
#sleep 15
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"
SCRIPT=${1:-"cluster_test.py"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTFILE=${3:-"sample_partistioned"}
LOCAL_RESULT=${4:-"local_result"}
FINAL_RESULT=${5:-"result.tsv"}
#read -p "wait"
export SPARK_DIST_CLASSPATH=$(hadoop classpath)
hdfs dfs -rm -r $OUTFILE
PYSPARK_PYTHON=$(readlink -f $(which python)) /home/bbkruit/spark-2.1.2-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/nltk_env/bin/python \
--conf spark.yarn.appMasterEnv.NLTK_DATA=./  \
--master yarn \
--deploy-mode cluster \
--num-executors 100 \
--executor-memory 2G \
--archives ~/anaconda3/envs/nltk_env.zip#VENV,tokenizers.zip#tokenizers,taggers.zip#taggers,1.zip#STANFORD \
--files stanford-ner.jar#JARFILE \
$SCRIPT $INFILE $OUTFILE $ES_NODE:$ES_PORT
kill $ES_PID
hdfs dfs -cat $OUTFILE"/*" > $LOCAL_RESULT
python3 transformer.py $LOCAL_RESULT $FINAL_RESULT
python3 score.py data/sample.annotations.tsv $FINAL_RESULT
#kill $ES_PID
