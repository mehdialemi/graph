#!/usr/bin/env bash

$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/start-dfs.sh

$SPARK_HOME/sbin/stop-all.sh
$SPARK_HOME/sbin/start-all.sh
