#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/kcore.conf"

function kcore {

    kValues=("$@")
    for k in "${kValues[@]}"
    do
            bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
    done
}

echo "youtube" > bin/inputs
kcore {4,40,51}

echo "cit-patents" > bin/inputs
kcore {4,40,64}

echo "soc-liveJournal" > bin/inputs
kcore {4,40,80,160,372}

echo "orkut" > bin/inputs
kcore {4,40,80,160,253}

echo "friendster" > bin/inputs
kcore {4,40,80,160,304}

echo "twitter" > bin/inputs
kcore {4,40,80,160,2647}
#done
