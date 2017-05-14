#!/usr/bin/env bash

export GRAPH_INPUT="soc-LiveJournal"
export PARTITIONS=150

bin/run_experiments.sh KT_TRIANGLE_COMP 5