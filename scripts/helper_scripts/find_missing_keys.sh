KEY_PREFIX=$1
KEY_FILE=$2
DATA_FILE=$3

keys=( $(grep $1 $2 | awk '{print $1}' | cut -b 5-) )

for key in "${keys[@]}"
do
  if ! grep -o $key $3 > /dev/null 2>&1; then
    echo $key
  fi
done
