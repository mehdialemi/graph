#!/usr/bin/env bash

jar="target/subgraph-mining-1.0-jar-with-dependencies.jar"
conf="bin/pattern/application.conf"
graph=`cat ${conf}  | grep targetGraph | cut -d '=' -f 2 | xargs echo -e`
partitions=`cat ${conf} | grep partitionNum | cut -d '=' -f 2 | xargs echo -e`
cores=`cat ${conf} | grep cores | cut -d '=' -f 2 | xargs echo -e`

#LOG_DIR="logs/pattern"
#mkdir $LOG_DIR
#log_name=`date +%Y-%m-%d.%H.%M.%S`
#log_file="$LOG_DIR/$log_name"