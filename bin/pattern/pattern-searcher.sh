#!/usr/bin/env bash

source $PWD/bin/pattern/pattern-env.sh

cmd="$SPARK_HOME/bin/spark-submit --class ir.ac.sbu.graph.spark.pattern.search.QueryMatcher $jar $conf"
echo "running $cmd"
#nohup ~/spark-2.2/bin/spark-submit --class $MAIN_CLASS $JAR_FILE $CONF_FILE > $log_file 2>&1 &
