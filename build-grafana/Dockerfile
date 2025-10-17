ARG GRAFANA_VERSION
FROM grafana/grafana:${GRAFANA_VERSION}

# RUN grafana-cli plugins install hamedkarbasi93-kafka-datasource
# RUN chown -R grafana:grafana /var/lib/grafana/plugins

COPY dashboard.yml /etc/grafana/provisioning/dashboards
COPY datasource.yml /etc/grafana/provisioning/datasources
COPY debezium-dashboard.json /etc/grafana/provisioning/dashboards/dashboards/debezium-dashboard.json
COPY brokers-dashboard.json /etc/grafana/provisioning/dashboards/dashboards/brokers-dashboard.json
COPY wip.json /etc/grafana/provisioning/dashboards/dashboards/wip.json
COPY debezium-mysql-connector-dashboard.json /etc/grafana/provisioning/dashboards/dashboards/debezium-mysql-connector-dashboard.json
COPY prometheus_ds.yml /etc/grafana/provisioning/datasources
