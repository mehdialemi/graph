#!/usr/bin/env bash

export GRAPH_INPUT="liveJournal"
bin/run_all.sh

export GRAPH_INPUT="soc-LiveJournal"
bin/run_all.sh

export GRAPH_INPUT="orkut"
bin/run_all.sh

export GRAPH_INPUT="friendster"
bin/run_all.sh

export GRAPH_INPUT="twitter"
bin/run_all.sh

export GRAPH_INPUT="c2014*"
bin/run_all.sh

export GRAPH_INPUT="c2012*"
bin/run_all.sh