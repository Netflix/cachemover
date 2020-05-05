#!/bin/bash
if [ $# -ne 1 ]; then
    echo "Usage: [name of the ASG starting with 'evcache']"
    exit 1
fi

echo "" > instance_ids.txt;
oq-lin $1 | awk '{print $6 "+" $2}' | egrep "^evcache" | awk -F"+" '{print $2}' > instance_ids.txt
cmd=`wc -l instance_ids.txt`;
echo "Going to download and build native dumper in $cmd instances"

clone_cmd="git clone https://stash.corp.netflix.com/scm/evcache/native-cache-dumper.git;cd native-cache-dumper"
install_deps_cmd="sudo ./scripts/build_scripts/install_deps.sh"
build_cmd="mkdir build;cd build;cmake ..;make"

for i in `cat instance_ids.txt`;
do
        ((count++))
        echo $i
        cmd1="oq-ssh $i ($clone_cmd;$install_deps_cmd;$build_cmd)"
        $cmd1 &
        if [ $(($count % 50)) == 0 ]; then
                wait $!
        fi
done
echo "sleeping till the last process is done"
wait $!
echo "all done"
