#!/bin/bash
cd runner
#cp ../application-console.properties debezium-server/config/application.properties
cp ../application-exasol.properties debezium-server/config/application.properties
cp ../Exasol_JDBC-25.2.4/*.jar debezium-server/lib/
cd debezium-server
rm /tmp/offsets_connector-stock.dat
rm /tmp/schemahistory_connector-stock.dat
./run.sh
