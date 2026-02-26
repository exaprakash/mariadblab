# Exasol sink for Debezium server
## Context
Debezium server is standalone application providing CDC pipeline functionality.
Debezium server is built on quarkas framework and jakarta cdi.
Quarkas framework creates build time skinned runner with the sink and connectors required.
This repo aims at adding the exasol sink to Debezium server.
### Run instructions
Copy the example exasol application properties and customize as needed.
```
cp application-exasol.properties.example application-exasol.properties
```
Run the build_run.sh, which adds the exasol sink to debezium server and builds it.
```
./build_run.sh
```
Go to teh runner folder to see the customized Debezium server running
```
cd runner
tail -f logs/debezium.log
```