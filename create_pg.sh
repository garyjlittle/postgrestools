#!/bin/bash -x

while getopts "s:d:v" Option
do
    case $Option in
        d   )   DEST=$OPTARG ;;
        s   )   SF=$OPTARG ;;
        v   )   VERBOSE=1 ;;
    esac
done
if [ ! -z $DEST  ]; then echo "The Destination is $DEST" ; fi

if [ ! -z $SF ]  ; then
    echo "The Scale Factor is $SF" 
else
    SF=100
fi


DB_PREFIX=pgbench_sf
DB_NAME=$DB_PREFIX$SF
PGUSER=postgres
echo DB Name is $DB_NAME

if [[ $(whoami) != $PGUSER ]] ; then
    echo You need to be $PGUSER to continue
    #exit here
fi

echo Creating the DB $DB_NAME schema
createdb $DB_NAME

echo Initializing DB with Scale Factor $SF
pgbench -i -s $SF $DB_NAME
