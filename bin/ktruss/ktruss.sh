#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

function ktruss {
    k=$1
    p=$2
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 5 $k 1000
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 0 $k 1000
}
echo "youtube" > bin/inputs
ktruss 19 20
#
echo "cit-patents" > bin/inputs
ktruss 36 34
#
echo "soc-liveJournal" > bin/inputs
ktruss  362 132
#
echo "orkut" > bin/inputs
ktruss 78  159
#
echo "friendster" > bin/inputs
ktruss 39   492

echo "twitter" > bin/inputs
ktruss 1998 1157