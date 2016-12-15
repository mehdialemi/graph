#!/usr/bin/env bash

heapSize="100g"
newRatio=2
maxPause=400
export BASEDIR=$(dirname "$0")
debug="-XX:+PrintGC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:$BASEDIR/gc.log"
export GC_OPTIONS="-XX:+UseParallelGC  -Xmx$heapSize -Xms$heapSize -XX:NewRatio=$newRatio -XX:MaxGCPauseMillis=$maxPause $debug"
export JAR_PATH="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
#export INPUT="friendster"
#export INPUT="twitter"
export INPUT="live-journal"
#export INPUT="soc-LiveJournal"
#export INPUT="cit-Patents.txt"
