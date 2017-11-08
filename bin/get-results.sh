#!/usr/bin/env bash

typeset -A config
config=(
    [jar]="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
    [limit]="6"
    [host]="alemi-1"
    [inputs]="$PWD/bin/inputs"
    [app]="kcore"
)

while read -r line
do
    conf_name=`echo "$line" | cut -d= -f 1`
    config[$conf_name]=`echo "$line" | cut -d= -f 2`
    conf_value=${config[$conf_name]}
    echo "$conf_name = $conf_value"
done < bin/result.conf

input=$(head -n 1 ${config[inputs]})

if [ -z "$1" ]
  then
    app="app"
  else
    app=$1
fi

java -cp ${config[jar]} ir.ac.sbu.graph.utils.AnalyzeResultsRestClient ${config[host]} ${config[limit]} logs/${config[app]}/${input}
