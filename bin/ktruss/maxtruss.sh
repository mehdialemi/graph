#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

function maxTruss {
    maxK=$1
    p=$2
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.MaxTrussTSetRange" $CONF_FILE 120 $p $maxK
}

echo "youtube" > bin/inputs
maxTruss 20 40

echo "cit-patents" > bin/inputs
maxTruss 40 102

echo "soc-liveJournal" > bin/inputs
maxTruss 400    330

echo "orkut" > bin/inputs
maxTruss 100 530

echo "friendster" > bin/inputs
maxTruss 40 984

echo "twitter" > bin/inputs
maxTruss 2000 5790







