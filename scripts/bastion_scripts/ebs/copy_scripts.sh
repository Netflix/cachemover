#!/bin/bash

if [ $# -ne 1 ]; then
    echo "need two arguments starting with evcache"
    exit 1
fi

oq-lin $1 | awk '{print $6 "+" $2}' | egrep $1 | awk -F"+" '{print $2}' > upgrade_$1.txt
echo "copying ebs scripts to instances in $1 asg"
count=0
BACK_PID=0
for i in `cat upgrade_$1.txt`;
do
        ((count++))
        echo $i
        oq-scp /home/pkarumanchi/scripts/ebs/to_checkin/nc_dump_ebs.py $i:/tmp &
	BACK_PID=$!
	echo "process id is $BACK_PID"
        if [ $(($count % 20)) == 0 ]; then
                wait $BACK_PID
        fi
        cmd="oq-ssh $i (sudo apt install -y python-pip; sudo pip install boto3)"
        $cmd
done
echo "sleeping till the last process $BACK_PID is done"
wait $BACK_PID
echo "done copying the script to instances"
