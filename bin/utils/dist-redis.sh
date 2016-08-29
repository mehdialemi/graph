#!/usr/bin/env bash

export PARTITIONS=100
export GRAPH_INPUT="liveJournal"
bin/run_experiments_CC.sh Dist_Redis
