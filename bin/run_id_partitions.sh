#!/bin/bash

run_command() {
    name=`echo $1 | cut -d ' ' -f 2`
        nohup $1 > logs/$name-$2.log 2>&1
}

input="com-friendster.ungraph.txt"
#input="crawlgraph2012*"

if [ ! -d "logs" ]; then
    mkdir logs
fi

bakdir=logs/bak/`date +%s`
mkdir $bakdir
mv logs/*.log $bakdir

p=1200
for i in {1..3}
do
	run_command "bin/submit.sh GCC_Id $input $p" $i
        sleep 3

	run_command "bin/submit.sh LCC_Id $input $p" $i
        sleep 3
	
	run_command "bin/submit.sh TC_Id $input $p" $i
        sleep 3

        p=$(( p*2 ))
done
