#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

function ktruss {
    k=$1
    p=$2
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 5 $k 1000
    bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 $p 0 $k 1000
}
echo "youtube" > bin/inputs
ktruss  4   20
ktruss  19  20
##
echo "cit-patents" > bin/inputs
ktruss  4   34
ktruss  36  34
##
echo "soc-liveJournal" > bin/inputs
ktruss  4 132
ktruss  40 132
ktruss  80 132
ktruss  160 132
ktruss  362 132
##
echo "orkut" > bin/inputs
ktruss  4   159
ktruss  40  159
ktruss 78  159
##
echo "friendster" > bin/inputs
ktruss  4   492
ktruss  39   492

echo "twitter" > bin/inputs
ktruss  1998    1157
ktruss  4   1157
ktruss  40  1157
ktruss  80  1157
ktruss  160 1157