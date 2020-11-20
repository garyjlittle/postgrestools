#!/bin/bash

DB_PREFIX=pgbench_sf
SF=100
DB_NAME=$DB_PREFIX$SF
PGUSER=postgres
echo DB Name is $DB_NAME

if [[ $(whoami) != $PGUSER ]] ; then
    echo You need to be $PGUSER to continue
    #exit here
fi

