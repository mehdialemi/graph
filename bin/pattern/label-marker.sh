#!/usr/bin/env bash

source $PWD/bin/pattern/pattern-env.sh
LOG="$PWD/logs/${graph}-p${partitions}-c${cores}-pattern-index.log"

cmd="$SPARK_HOME/bin/spark-submit --class ir.ac.sbu.graph.spark.pattern.label.LabelManager $jar $conf"
echo "running $cmd > $LOG"

nohup sh $cmd > $LOG 2>&1 &
tail -f "$LOG"

#$SPARK_HOME/bin/spark-submit --class $class $jar $conf
#nohup ~/spark-2.2/bin/spark-submit --class $MAIN_CLASS $JAR_FILE $CONF_FILE > $log_file 2>&1 &
