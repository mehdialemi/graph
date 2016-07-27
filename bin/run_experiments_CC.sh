#!/bin/bash

run_command() {
    name=`echo $1 | cut -d ' ' -f 2`
	nohup $1 > logs/$name-$2.log 2>&1
}

input="com-friendster.ungraph.txt"
partitions=1000

if [ ! -d "logs" ]; then
    mkdir logs
fi

bakdir=logs/bak/`date +%s`
mkdir $bakdir
mv logs/*.log $bakdir

for i in {1..5}
do
    run_command "bin/submit.sh GCC_Deg $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh GCC_Id $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh GCC_GraphX $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh GCC_NodeIter $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh LCC_Deg $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh LCC_Id $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh LCC_GraphX $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh TC_Deg $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh TC_Id $input $partitions" $i
	sleep 3
	
	run_command "bin/submit.sh TC_GraphX $input $partitions" $i
	sleep 3
    
	run_command "bin/submit.sh TC_NodeIter $input $partitions" $i
	sleep 3
done
