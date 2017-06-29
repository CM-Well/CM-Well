#!/bin/sh
echo "This will WIPE all data and CANNOT BE UNDONE! Are you sure? Type YeS in order to continue"
read answer
if [ $answer != 'YeS' ]; then echo "Nothing was done"; exit 1; fi