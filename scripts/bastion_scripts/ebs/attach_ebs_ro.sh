#!/bin/bash

if [ $# -ne 2 ]; then
    echo "need two arguments - dest asg, src asg"
    exit 1
fi

oq-lin $1 | awk '{print $6 "+" $2}' | egrep $1 | awk -F"+" '{print $2}' > upgrade_$1.txt
echo "listing ebs volumes for instances associated with $1 asg"
count=0
BACK_PID=0
for i in `cat upgrade_$1.txt`;
do
        ((count++))
        #echo $i
	cmd="oq-ssh $i (python /tmp/nc_dump_ebs.py --action_type attach-vol-ro --asg $2)"
	$cmd
done
echo "done listing ebs volumes in all the instances"
