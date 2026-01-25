sudo -u postgres pg_restore -j 8 -F d /mnt/pgbackup/tpcc/ -d $XRAY_PG_DB_NAME
