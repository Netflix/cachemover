DIR=$1
FILE_PREFIX=$2

TOTAL_NUM_LINES=0
for fname in $DIR/$FILE_PREFIX*; do
  NUM_LINES=$(wc -l $fname | awk '{print $1}')
  TOTAL_NUM_LINES=$(( TOTAL_NUM_LINES + NUM_LINES ))
  echo $NUM_LINES
  LAST_LINE=$(tail -n 1 $fname)
  if [[ ! $LAST_LINE =~ $REGEX_MATCH ]]; then
    echo $fname " is corrupt"
  fi
done

echo $TOTAL_NUM_LINES
