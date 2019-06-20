#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"
MAIN_CLASS="ir.ac.sbu.graph.spark.search.PatternCounter"
SPARK_HOME=""
SPARK_MASTER="alemi-1"
JAR_FILE="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
LOG_DIR="logs/pattern/"


echo "Running search with graph: $graph, pattern: $pattern"

command="$SPARK_HOME/bin/spark-submit --class $MAIN_CLASS --master $SPARK_MASTER:7077 $JAR_FILE search.config"
log_name=`date +%Y-%m-%d.%H.%M.%S`
log_file="$LOG_DIR/$log_name"

echo "Running $command"
nohup $command > $log_file 2>&1 &
tail -f $log_file
