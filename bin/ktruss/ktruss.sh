#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

function ktruss {
    kValues=("$@")
    for k in "${kValues[@]}"
    do
        bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 1000 $k 100
        bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE 120 0 $k 100
    done
}

echo "youtube" > bin/inputs
ktruss {4,17}

echo "cit-patents" > bin/inputs
ktruss {4,36}

echo "soc-liveJournal" > bin/inputs
ktruss {4,40,80,160,362}

echo "orkut" > bin/inputs
ktruss {4,40,75}

echo "friendster" > bin/inputs
ktruss {4,40,129}

echo "twitter" > bin/inputs
ktruss {4,40,80,160,1998}