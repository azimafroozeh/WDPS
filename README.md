first run this:

export SPARK_DIST_CLASSPATH=$(hadoop classpath)

then delete testazim1 if it's exist 

hadoop fs -rm -r testazim1
