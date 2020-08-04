ord() {
  LC_CTYPE=C printf '%d' "'$1"
}

DIR=$1
#FILE_PREFIX=$2
for fname in $DIR/*; do
  RET=$(md5sum $fname)
  REAL_MD5=$(sed -e 's/\s.*$//' <<< $RET)
  FILE_MD5=$(sed "s/^.*_\([^_]*\)$/\1/" <<< $RET)
  FILE_MD5_LOWER=$(sed -e "s/\(.*\)/\L\1/" <<< $FILE_MD5)

  if [[ "$FILE_MD5_LOWER" != "$REAL_MD5" ]]; then
    echo $fname " IS CORRUPT"
    echo $FILE_MD5_LOWER
    echo $REAL_MD5
    echo ""
  fi
done
