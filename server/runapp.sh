docker rmi exarhel/debezium-server-console
docker build -t exarhel/debezium-server-console .
docker run -it --detach --entrypoint /bin/bash --name debezium -p 8080:8080 quay.io/debezium/server
docker cp application.properties debezium:/debezium/config/application.properties