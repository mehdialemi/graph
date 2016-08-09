#!/bin/bash

run_command() {

    nohup $1 > $2 2>&1
}

#input="com-friendster.ungraph.txt"
#input="com-amazon.ungraph.txt"
input="liveJournal"
#input="twitter.txt"

if [ ! -d "logs" ]; then
    mkdir logs
fi

if [ ! -d "logs/old" ]; then
        mkdir logs/old
fi

d=`date +%s`

p=240
IFS=',' read -ra TASKS <<< $1
for task in "${TASKS[@]}"; do
     logDir="logs/$task/$input"
    if [ ! -d $logDir ]; then
        mkdir -p $logDir
    fi

    SECONDS=0
    d=`date +%s`
    run_command "bin/submit.sh $task $input $p"  $logDir/"$d.log"
    echo "`LANG=de_DE date` Task=$task, Input=$input, Partitions=$p, Duration=$SECONDS, Log=$logDir/$d.log" >> logs/results.txt
    sleep 3
done
