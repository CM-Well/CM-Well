#!/bin/bash

tokenFileName=~/dev/uploadTool/token-$1
[ -e "$tokenFileName" ] || touch $tokenFileName -d 2000-01-01

tokenAge=$(($(date +%s) - $(date +%s -r $tokenFileName)))
if [ $tokenAge -gt 3600 ]
then
    token=$(curl -s "http://${1:-localhost:9000}/_login" -u root | jq -r ".token")
    echo $token > $tokenFileName
else
    token=$(cat $tokenFileName)
fi

# Create AppCache Manifest according to file list and commit hash

commit=$(git rev-parse HEAD)
fileList=$(find -type f | grep -vE 'index.html|cmwell.appcache' | sed 's/^\./\/meta\/app\/main/')
echo -e "CACHE MANIFEST\n\n# $commit\n\nCACHE:\n$fileList\n\nNETWORK:\n*" > cmwell.appcache

# Upload SPA

for file in $( find -newermt `date +%Y-%m-%d` -type f | grep -v ".idea\|target" | sed 's/^.\///' ); do
    echo -n "  Uploading $file "

    # this for loop is only to show off
    for i in $(seq 35 -1 $(expr length $file)); do echo -n .; sleep .005; done; echo -n " "

    case "$(echo ${file##*.} | tr '[A-Z]' '[a-z]')" in
      html) mime=text/html ;;
      js) mime=application/javascript ;;
      jsx) mime=application/javascript ;;
      css) mime=text/css ;;
      ico) mime=image/x-icon ;;
      jpg) mime=image/jpeg ;;
      gif) mime=image/gif ;;
      png) mime=image/png ;;
      svg) mime=image/svg+xml ;;
      md) mime=text/x-markdown ;;
      woff2) mime=font/woff2 ;;
      appcache) mime=text/cache-manifest ;;
      *)   mime=text/plain ;;
    esac
    
    curl -XPOST "http://${1:-localhost:9000}/meta/app/main/$file" -H "X-CM-WELL-TOKEN:$token" -H "X-CM-Well-Type: File" -H "Content-Type: $mime" --data-binary @$file
    echo
done
echo
date +%T
echo

