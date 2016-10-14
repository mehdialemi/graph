#!/usr/bin/env bash

export BASEDIR=$(dirname "$0")
. $BASEDIR/../subgraph-env.sh

echo "GC = $GC_OPTIONS"
echo "JAR = $JAR_PATH"
java $GC_OPTIONS -cp $JAR_PATH ir.ac.sbu.graph.ktruss.sequential.KTrussSequential "$HOME/graph-data/$INPUT" 4
