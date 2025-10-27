#!/bin/bash
rm -fr runner debezium-server
git clone https://github.com/debezium/debezium-server
cp -R debezium-server-console/ debezium-server/
cp -R debezium-server-exasol/ debezium-server/
./update_pom.sh
./update_bom_pom.sh
./update_dist_pom.sh
cd debezium-server/
mvn clean install -pl debezium-server-console -am -DskipTests  -DskipITs
mvn clean install -pl debezium-server-exasol -am -DskipTests  -DskipITs -Dcheckstyle.skip=true
mvn clean package -pl debezium-server-dist -Passembly
mkdir ../runner
tar -xvzf debezium-server-dist/target/debezium-server-dist-3.4.0-SNAPSHOT.tar.gz -C ../runner
cd ../runner
#cp ../application-console.properties debezium-server/config/application.properties
cp ../application-exasol.properties debezium-server/config/application.properties
cp ../Exasol_JDBC-25.2.4/*.jar debezium-server/lib/
cd debezium-server
rm /tmp/offsets_connector-stock.dat
rm /tmp/schemahistory_connector-stock.dat
./run.sh
