#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

function ktruss {
    k=$1
    p=$2
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 5 $k 100
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 0 $k 100
}

echo "youtube" > bin/inputs
ktruss 17 60

echo "cit-patents" > bin/inputs
ktruss 36 120

echo "soc-liveJournal" > bin/inputs
ktruss 362 360

echo "orkut" > bin/inputs
ktruss 78  480

echo "friendster" > bin/inputs
ktruss 39   1200

echo "twitter" > bin/inputs
ktruss 1998 3600