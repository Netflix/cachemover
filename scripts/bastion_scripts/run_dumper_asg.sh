#!/bin/bash
if [ $# -ne 2 ]; then
    echo "Usage: [name of the ASG starting with 'evcache'] [output_dir]"
    exit 1
fi

echo "" > instance_ids.txt;
oq-lin $1 | awk '{print $6 "+" $2}' | egrep "^evcache" | awk -F"+" '{print $2}' > instance_ids.txt
cmd=`wc -l instance_ids.txt`;
echo "Going to run the native dumper in $cmd instances"

output_dir=$2

bin="~/native-cache-dumper/bin/memcache-dumper"
options="-i 127.0.0.1 -p 11211 -t 4 -b 67108864 -m 1073741824 -k 67108864 -d 268435456 -o $output_dir"
stdout_redir="dumper_stdout"
for i in `cat instance_ids.txt`;
do
        ((count++))
        echo $i
        cmd1="oq-ssh $i ($bin $options > $stdout_redir)"
        $cmd1 &
        if [ $(($count % 50)) == 0 ]; then
                wait $!
        fi
done
echo "sleeping till the last process is done"
wait $!
echo "all done"
