#!/bin/bash

if [ $# -ne 1 ]; then
    echo "need two arguments starting with evcache"
    exit 1
fi

oq-lin $1 | awk '{print $6 "+" $2}' | egrep $1 | awk -F"+" '{print $2}' > upgrade_$1.txt
echo "deleting ebs volumes from these $2 for $(wc -l upgrade_$1.txt) instances"
count=0
BACK_PID=0
for i in `cat upgrade_$1.txt`;
do
        ((count++))
        echo $i
	cmd="oq-ssh $i (python /tmp/nc_dump_ebs.py --action_type delete-vol)"
	$cmd
done
echo "done deleting ebs volumes in all the instances"
