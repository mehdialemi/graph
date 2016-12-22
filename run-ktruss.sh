#!/usr/bin/env bash

for graph in youtube dblp as-skitter gowalla penn94 cit-Patents.txt live-journal soc-LiveJournal
do
	echo "Select $graph"
	for k in {3..100}
	do
		echo "Test by k $k"
		bin/ktruss/ktruss-pscript.sh $graph $k 12 >> $graph.log
		echo "**********************************" >> $graph.log
		echo >> $graph.log
		echo >> $graph.log
	done
done
