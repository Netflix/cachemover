#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Usage: [name of the ASG starting with 'evcache']"
    exit 1
fi

echo "" > instance_ids.txt;
oq-lin $1 | awk '{print $6 "+" $2}' | egrep "^evcache" | awk -F"+" '{print $2}' > instance_ids.txt
cmd=`wc -l instance_ids.txt`;
echo "Going to update native dumper in $cmd instances"

cd_cmd="cd native-cache-dumper/build"
git_cmd="git fetch origin;git rebase origin/master"
build_cmd="cmake ..;make"

for i in `cat instance_ids.txt`;
do
        ((count++))
        echo $i
        cmd1="oq-ssh $i ($cd_cmd;$git_cmd;$build_cmd)"
        $cmd1 &
        if [ $(($count % 50)) == 0 ]; then
                wait $!
        fi
done
echo "sleeping till the last process is done"
wait $!
echo "all done"
