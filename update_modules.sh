#!/bin/bash

BASE_URI="s3://dataincubator-course/datacourse/latest"

get_latest_bak() {
  if [ ! -e "$1.bak" ]; then
    return 0
  fi

  COUNTER=1
  while [ -e "$1.bak.$COUNTER" ]; do
    COUNTER=$(( $COUNTER + 1 ))
  done
  return $COUNTER
}

move_to_backup() {
  directory=$1
  get_latest_bak $directory
  index=$?
  if [ $index == 0 ]; then
    mv $directory $directory.bak
  else
    mv $directory $directory.bak.$index
  fi
}

if [ -z "$1" ]; then
  echo "Usage: ./update_modules.sh [module_name]"
  echo "Or, for all modules: ./update_modules.sh all"
  exit 1
else
  if [ "$1" == "all" ]; then
    directories=$(ls -d */ | grep -v 'bak' | cut -d / -f 1)
    for directory in $directories; do
      # if directory doesnt exist, it's something the student put there
      if [ -z "$(aws s3 ls ${BASE_URI}/${directory})" ]; then
        continue
      else
        aws s3 cp --recursive ${BASE_URI}/${directory} $directory.new
      fi
    done
  else
    aws s3 cp --recursive ${BASE_URI}/$1 $1.new
  fi
fi

echo "=========================="
if [ "$1" != "all" ]; then
  echo "Done. New content is in $1.new"
else
  echo "Done. New content is in the .new directory for each module."
fi
echo "=========================="
