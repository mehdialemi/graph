#!/usr/bin/env bash

export BASEDIR=$(dirname "$0")
. $BASEDIR/../subgraph-env.sh

echo "GC = $GC_OPTIONS"
echo "JAR = $JAR_PATH"
k=$1
threads=$2
method=$3
java $GC_OPTIONS -cp $JAR_PATH ir.ac.sbu.graph.ktruss.parallel.KTrussParallel "$HOME/graph-data/$INPUT" $k $threads $method
