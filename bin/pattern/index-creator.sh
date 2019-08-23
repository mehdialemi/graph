#!/usr/bin/env bash

source bin/pattern/pattern-env.sh
cmd="$SPARK_HOME/bin/spark-submit --class ir.ac.sbu.graph.spark.pattern.index.GraphIndex $jar $conf"
echo "running $cmd"

sh $cmd

#$SPARK_HOME/bin/spark-submit --class $class $jar $conf
#nohup ~/spark-2.2/bin/spark-submit --class $MAIN_CLASS $JAR_FILE $CONF_FILE > $log_file 2>&1 &
