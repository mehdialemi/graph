#!/usr/bin/env bash

export PARTITIONS=100
export GRAPH_INPUT="liveJournal"
bin/run_experiments_CC.sh Dist

export GRAPH_INPUT="cit-Patents"
bin/run_experiments_CC.sh Dist

export GRAPH_INPUT="soc-LiveJournal"
bin/run_experiments_CC.sh Dist

export PARTITIONS=500
export GRAPH_INPUT="orkut"
bin/run_experiments_CC.sh Dist

export GRAPH_INPUT="friendster"
bin/run_experiments_CC.sh Dist

export GRAPH_INPUT="twitter"
bin/run_experiments_CC.sh Dist

export PARTITIONS=1000

export GRAPH_INPUT="c2014*"
bin/run_experiments_CC.sh Dist

export GRAPH_INPUT="c2012*"
bin/run_experiments_CC.sh Dist