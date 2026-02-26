# Stage 1: Build Debezium tarball inside Docker
FROM registry.access.redhat.com/ubi8/openjdk-21 AS builder
USER root
RUN microdnf -y install git maven gzip && microdnf clean all

WORKDIR /app

RUN microdnf remove -y maven || true && \
    microdnf install -y wget tar gzip && \
    wget -q https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/apache-maven-3.9.9-bin.tar.gz && \
    tar -xzf apache-maven-3.9.9-bin.tar.gz -C /opt && \
    ln -s /opt/apache-maven-3.9.9/bin/mvn /usr/local/bin/mvn && \
    mvn -v


# Clone dependency
RUN git clone https://github.com/debezium/debezium.git && \
    cd /app/debezium && \
    pwd && \
    mvn clean install -DskipITs -DskipTests && \
    cd /app && rm -fr /app/debezium

# Clone repo and build inside container
RUN git clone https://github.com/debezium/debezium-server
COPY debezium-server-console/ /app/debezium-server/debezium-server-console/
COPY update_pom.sh update_bom_pom.sh update_dist_pom.sh /app/
RUN chmod +x /app/update_*.sh

RUN /app/update_pom.sh && \
    /app/update_bom_pom.sh && \
    /app/update_dist_pom.sh && \
    cd /app/debezium-server && \
    mvn clean install -pl debezium-server-console -am -DskipTests -DskipITs && \
    mvn clean package -pl debezium-server-dist -Passembly

# Stage 2: Runtime image
FROM registry.access.redhat.com/ubi8/openjdk-21
ENV DEBEZIUM_VERSION=3.4.0-SNAPSHOT \
    SERVER_HOME=/debezium
USER root
RUN microdnf clean all && mkdir -p $SERVER_HOME && chmod 777 $SERVER_HOME
USER jboss

COPY --from=builder /app/debezium-server/debezium-server-dist/target/debezium-server-dist-$DEBEZIUM_VERSION.tar.gz /tmp/debezium.tar.gz
RUN tar xzf /tmp/debezium.tar.gz -C $SERVER_HOME --strip-components=1 && rm /tmp/debezium.tar.gz

WORKDIR $SERVER_HOME
EXPOSE 8080
VOLUME ["/debezium/config","/debezium/data"]
CMD ["./run.sh"]
