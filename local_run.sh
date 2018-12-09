hadoop fs -rm -r testazim2
rm result

SCRIPT=${1:-"local_test.py"}
INFILE=${2:-"hdfs:///user/bbkruit/sample.warc.gz"}
OUTFILE=${3:-"testazim2"}

hadoop fs -rm -r $OUTFILE
# This assumes there is a python virtual environment in the "venv" directory
PYSPARK_PYTHON=$(readlink -f $(which python)) /home/bbkruit/spark-2.1.2-bin-without-hadoop/bin/spark-submit \
--conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./VENV/nltk_env/bin/python \
--conf spark.yarn.appMasterEnv.NLTK_DATA=./  \
--master local[8] \
--archives ~/anaconda3/envs/nltk_env.zip#VENV,tokenizers.zip#tokenizers,taggers.zip#taggers,1.zip#STANFORD \
--files stanford-ner.jar \
$SCRIPT $INFILE $OUTFILE
hdfs dfs -cat $OUTFILE"/*" > "result"
