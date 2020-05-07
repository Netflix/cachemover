#!/bin/bash

if [ $# -ne 2 ]; then
    echo "need two arguments - asg, volume size"
    exit 1
fi
size=$2
oq-lin $1 | awk '{print $6 "+" $2}' | egrep $1 | awk -F"+" '{print $2}' > upgrade_$1.txt
echo "creating ebs volumes and attaching them on instances in $1 asg"
count=0
for i in `cat upgrade_$1.txt`;
do
        ((count++))
        echo $i
	cmd="oq-ssh $i (python /tmp/nc_dump_ebs.py --action_type create-attach-vol --size $size)"
	$cmd
	cmd="oq-ssh $i (cat /tmp/ebs-vol-exists)"
	$cmd >> /home/pkarumanchi/scripts/ebs/ebs_vol_$1.txt
done
echo "done attaching ebs volumes in all the instances"
