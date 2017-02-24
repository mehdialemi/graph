#!/usr/bin/env bash

export GRAPH_INPUT="friendster"
export PARTITIONS=900

bin/run_experiments.sh KT_SPARK_UPDATE 5