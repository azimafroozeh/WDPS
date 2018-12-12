#!/usr/bin/env bash
ES_PORT=9200
ES_BIN=$(realpath ~/scratch/elasticsearch-2.4.1/bin/elasticsearch)

prun -o .es_log -v -np 1 ESPORT=$ES_PORT $ES_BIN </dev/null 2> .es_node &
echo "waiting 15 seconds for elasticsearch to set up..."
until [ -n "$ES_NODE" ]; do ES_NODE=$(cat .es_node | grep '^:' | grep -oP '(node...)'); done
ES_PID=$!
sleep 15
echo "elasticsearch should be running now on node $ES_NODE:$ES_PORT (connected to process $ES_PID)"
INFILE=${2:-"result"}
OUTFILE=${3:-"finlal"}
FINAL_RESULT=${5:-"result.tsv"}
python3 mention2entity.py $INFILE $OUTFILE $ES_NODE:$ES_PORT
python3 transformer.py $OUTFILE $FINAL_RESULT
python3 score.py data/sample.annotations.tsv $FINAL_RESULT
kill $ES_PID
