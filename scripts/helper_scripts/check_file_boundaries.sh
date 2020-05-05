ord() {
  LC_CTYPE=C printf '%d' "'$1"
}

REGEX_MATCH="(key=.* exp=(-1|[0-9]+) la=[0-9]+ cas=[0-9]+ fetch=(yes|no) cls=[0-9]+ size=[0-9]+|END)"

DIR=$1
FILE_PREFIX=$2
for fname in $DIR/$FILE_PREFIX*; do
  LAST_LINE=$(tail -n 1 $fname)
  if [[ ! $LAST_LINE =~ $REGEX_MATCH ]]; then
    echo $fname " is corrupt"
  fi
done
