#!/bin/bash
err() {
  printf '%s\n' "$1" >&2
  exit 1
}

while :; do
  case $1 in
    -d|--with-data)
      WITH_DATA="&with-data"
    ;;
    -o|--output)
      if [ "$2" ]; then
        OUTPUT=$2
      else
        err 'ERROR: "--output" reqquires an argument to be supplied!'
      fi
      shift
    ;;
    -p|--path)
      if [ "$2" ]; then
         CMW_PATH=$2
       else
         CMW_PATH="/"
       fi
       shift
    ;;
    -q|--qp)
      if [ "$2" ]; then
         QP="&qp=$2"
         shift
       else
         QP=""
       fi
    ;;
    -s|--host)
       if [ "$2" ]; then
         CMW_HOST=$2
       else
         err 'ERROR: "--host" reqquires an argument to be supplied!'
       fi
       shift
    ;;
    -h|--help)
      echo -e "Usage: cmw-iterate [OPTION]...\n"
      echo -e "cmw-iterate creates an iterator against CM-Well, and consumes it\n"
      echo "  -d, --with-data       Specifies whether to fetch infotons' data as well"
      echo "  -o, --output    <ARG> If supplied, writes the data to the specified file,"
      echo "                        and print out progress to STDOUT, otherwise output to STDOUT"
      echo "  -p, --path      <ARG> Specifies the path, or root if not supplied"
      echo "  -q, --qp        <ARG> Specifies qp argument"
      echo "  -s, --host      <ARG> Host to operate against (REQUIRED)"
      echo -e "  -h, --help            Shows this help message\n"
      exit 0
    ;;
    *)
      break
  esac
  shift
done

if [ -z "$CMW_HOST" ]; then
  err 'ERROR: "--host" is required!'
fi

ID=$(curl -s "$CMW_HOST$CMW_PATH?op=create-iterator$QP&session-ttl=60" | jq -r '.iteratorId')
OUT=$(curl -k --silent -L -w "\n%{http_code}" "$CMW_HOST/_iterate?session-ttl=60$WITH_DATA&format=json&iterator-id=$ID")
BODY="${OUT%$'\n'*}"
STATUS="${OUT##*$'\n'}"

if [ -z "$OUTPUT" ]; then
  echo $BODY | jq -c '.infotons[]'
else
  echo $BODY | jq -c '.infotons[]' > $OUTPUT
fi

ID=$(echo "$BODY" | jq -r '.iteratorId')
COUNT=$(echo "$BODY" | jq -r '.infotons|length')
TOTAL=$(echo "$BODY" | jq -r '.totalHits')
if ! [ -z "$OUTPUT" ]; then
  echo "total infotons to retrieve: $TOTAL"
fi
TOTAL=$(($TOTAL-$COUNT))
while [ \( $TOTAL -gt 0 \) -a \( $STATUS == 200 \) ]
do
  if ! [ -z "$OUTPUT" ]; then
    echo "infotons left to retrieve: $TOTAL"
  fi
  OUT=$(curl -k --silent -L -w "\n%{http_code}" "$CMW_HOST/_iterate?session-ttl=60$WITH_DATA&format=json&iterator-id=$ID")
  BODY="${OUT%$'\n'*}"
  STATUS="${OUT##*$'\n'}"
  if [ $STATUS == 200 ]
  then
    if [ -z "$OUTPUT" ]; then
      echo $BODY | jq -c '.infotons[]'
    else
      echo $BODY | jq -c '.infotons[]' >> $OUTPUT
    fi
    ID=$(echo "$BODY" | jq -r '.iteratorId')
    COUNT=$(echo "$BODY" | jq -r '.infotons|length')
    TOTAL=$(($TOTAL-$COUNT)) ;
  fi
done

if ! [ -z "$OUTPUT" ]; then
  if [ \( $STATUS -ne 200 \) -o \( $TOTAL -ne 0 \) ];then
    echo -e "status: $STATUS \nbody: $BODY"
  else
    echo "SUCCESS!"
  fi
fi
