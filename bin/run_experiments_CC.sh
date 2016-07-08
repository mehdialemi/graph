#!/bin/bash
.submit.sh

for i in {1..5}
do
	bin/submit.sh GCC_Deg ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh LCC_Deg ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh TC_Deg ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh GCC_Id ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh LCC_Id ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh TC_Id ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh GCC_GraphX ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh LCC_GraphX ~/graph-data/cit-Patents.txt 20
	sleep 3
	bin/submit.sh TC_GraphX ~/graph-data/cit-Patents.txt 20
	sleep 3
    bin/submit.sh GCC_NodeIter ~/graph-data/cit-Patents.txt 20
    sleep 3
    bin/submit.sh TC_NodeIter ~/graph-data/cit-Patents.txt 20
    sleep 3
done 
