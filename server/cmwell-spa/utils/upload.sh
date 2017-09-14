#!/bin/bash

# USAGE:
# The working directory will be (recursively) uploaded to /meta/app/main/
# An optional first parameter is the target cluster, without "http://". Default is localhost:9000.
# An optional seconds parameter is the target path in CM-Well. Default is /meta/app/main.
# e.g.: ~/myWebApp$ ~/upload.sh my-cmwell-cluster /example.org/myWebApp
# You can use this on multiple environments, tokens will be kept accordingly

# REQUIREMENTS:
# jq (sudo apt install jq)

# NOTES:
# - Only files changed in last day will be uploaded. If you wish to upload all files, regardless of their last modified date, omit the -newermt parameter below
# - If you want to ignore other files rather than .idea and targer, add them to the "grep -v" below.
# - If you have other extensions and mime types, please add them to the switch case below.
# - A known pitfall might be to place this upload.sh in same directory as the SPA content. Doing so will upload it to CM-Well as well, which is not harmful but unnecessary.

mkdir -p ~/.cmwell
tokenFileName=~/.cmwell/token-$1
[ -e "$tokenFileName" ] || touch $tokenFileName -d 2000-01-01

tokenAge=$(($(date +%s) - $(date +%s -r $tokenFileName)))
if [ $tokenAge -gt 3600 ]
then
    token=$(curl -s "http://${1:-localhost:9000}/_login" -u root | jq -r ".token")
    echo $token > $tokenFileName
else
    token=$(cat $tokenFileName)
fi

for file in $( find -newermt `date +%Y-%m-%d` -type f | grep -v ".idea\|target" | sed 's/^.\///' ); do
    echo -n "  Uploading $file "

    # this for loop is only to show off
    for i in $(seq 35 -1 $(expr length $file)); do echo -n .; sleep .005; done; echo -n " "

    case "$(echo ${file##*.} | tr '[A-Z]' '[a-z]')" in
      html) mime=text/html ;;
      js)       mime=application/javascript ;;
      jsx)      mime=application/javascript ;;
      css)      mime=text/css ;;
      ico)      mime=image/x-icon ;;
      jpg)      mime=image/jpeg ;;
      gif)      mime=image/gif ;;
      png)      mime=image/png ;;
      svg)      mime=image/svg+xml ;;
      md)       mime=text/x-markdown ;;
      woff2)    mime=font/woff2 ;;
      appcache) mime=text/cache-manifest ;;
      *)        mime=$(file -b --mime-type $file) ;;
    esac
    
    curl -XPOST "http://${1:-localhost:9000}${2:-/meta/app/main}/$file" -H "X-CM-WELL-TOKEN:$token" -H "X-CM-Well-Type: File" -H "Content-Type: $mime" --data-binary @$file
    echo
done
echo
date +%T
echo

