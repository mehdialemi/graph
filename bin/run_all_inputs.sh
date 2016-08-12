#!/usr/bin/env bash

export GRAPH_INPUT="liveJournal"
export PARTITIONS=150
bin/run_all.sh

export GRAPH_INPUT="soc-LiveJournal"
export PARTITIONS=300
bin/run_all.sh

export GRAPH_INPUT="orkut"
export PARTITIONS=500
bin/run_all.sh

export GRAPH_INPUT="friendster"
export PARTITIONS=2000
bin/run_all.sh

export GRAPH_INPUT="twitter"
export PARTITIONS=3000
bin/run_all.sh

export GRAPH_INPUT="c2014*"
export PARTITIONS=10000
bin/run_all.sh

export GRAPH_INPUT="c2012*"
export PARTITIONS=20000
bin/run_all.sh