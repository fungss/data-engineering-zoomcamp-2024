#!/bin/bash
set -e

TAXI_TYPE=${1} # "yellow"
YEAR=${2} # 2020

if [ -z "${TAXI_TYPE}" ] | [ -z "${YEAR}" ];
then
  echo "Usage: ./download_data.sh TAXI_TYPE YEAR"
  exit 1
fi

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  FILE_NAME="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
  URL="${URL_PREFIX}/${TAXI_TYPE}/${FILE_NAME}"

  LOCAL_PREFIX="./bucket/bronze/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_PATH="${LOCAL_PREFIX}/${FILE_NAME}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

done