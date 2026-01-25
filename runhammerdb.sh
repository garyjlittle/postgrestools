#!/usr/bin/env bash 
#
RUNID=$(date +'%Y-%m-%d_%H_%M_%S')
RUNDIRNAME=$RUNID-$DB-DB-$VU-VU-ALLWH-$ALLWH
echo "Make Directory "$RUNDIRNAME
mkdir $RUNDIRNAME
cp hammerdb-template.tcl $RUNDIRNAME/hammerdb.tcl
sed -i "s/vuset vu/vuset vu $VU/" $RUNDIRNAME/hammerdb.tcl
sed -i "s/diset tpcc pg_dbase/diset tpcc pg_dbase $DB/" $RUNDIRNAME/hammerdb.tcl
sed -i "s/diset tpcc pg_allwarehouse/diset tpcc pg_allwarehouse $ALLWH/" $RUNDIRNAME/hammerdb.tcl
exit



exit
ssh -q root@perf105-xray-postgres-server "cd /home/nutanix ; /home/nutanix/bin/show_xray_db_config_simple.sh" | tee $RUNID/dbinfo
ssh -q perf105-xray-postgres-client "cat /home/nutanix/output/hammerdb/runhammerdb-8disk-3-3.tcl"|rg "pg_dbase|rampup|duration| vu " | tee $RUNID/hammerdbinfo
ssh perf105-xray-postgres-client "cd /usr/local/bin/hammerdb ;sudo ./hammerdbcli auto  /home/nutanix/output/hammerdb/runhammerdb-8disk-3-3.tcl" | tee $RUNID/hammerdb_output
scp perf105-xray-postgres-client:/tmp/hammerdb.log $RUNID
scp perf105-xray-postgres-client:/tmp/hdbtcount.log $RUNID
