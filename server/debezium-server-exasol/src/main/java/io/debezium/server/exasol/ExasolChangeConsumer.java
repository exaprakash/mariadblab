package io.debezium.server.exasol;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.time.*;
import java.util.*;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Exasol sink compatible with Debezium Server (serialized JSON ChangeEvent).
 */
@Named("exasol")
@Dependent
public class ExasolChangeConsumer extends BaseChangeConsumer
        implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExasolChangeConsumer.class);
    public static final String EXASOL_CONNECTION_URL = "exasol.connection.url";
    public static final String EXASOL_CONNECTION_USERNAME = "exasol.connection.username";
    public static final String EXASOL_CONNECTION_PASSWORD = "exasol.connection.password";
    private Connection conn;
    private String url;
    private String username;
    private String password;
    private String tableName = "";
    private int currentHour = 0;
    private long currentHourRecordsIn = 0L;
    private static final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    void connect() {
        final Config config = ConfigProvider.getConfig();
        initWithConfig(config);
        LOGGER.info(">>> ConsoleChangeConsumer @PostConstruct triggered");
    }

    @VisibleForTesting
    void initWithConfig(Config config) {
        LOGGER.trace("initWithConfig invoked ");
        this.conn = null;
        this.url = config.getValue("exasol.connection.url", String.class);
        this.username = config.getValue("exasol.connection.username", String.class);
        this.password = config.getValue("exasol.connection.password", String.class);
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {

        // Create or reuse a file appender for console sink logging
        Path logFile = Paths.get("logs/cdc-exasol-consumer.json");
        try {
            if (!Files.exists(logFile.getParent())) {
                Files.createDirectories(logFile.getParent());
            }
            if (!Files.exists(logFile)) {
                Files.createFile(logFile);
            }
        }
        catch (IOException e) {
            LOGGER.error("Failed to initialize console sink log file", e);
            return;
        }
        // hourly counters
        int hour = (int) (System.currentTimeMillis() / 3_600_000L);
        if (hour != this.currentHour) {
            this.currentHour = hour;
            this.currentHourRecordsIn = records.size();
        }
        else {
            this.currentHourRecordsIn += records.size();
        }

        LOGGER.info("EXASOL Batch received - {} records, {} current hour records", 
                records.size(), this.currentHourRecordsIn);

        NanoTimer batchTimer = NanoTimer.start();

        // compaction result: keys are composite string keys; values are payload JsonNodes (the "payload")
        Map<String, JsonNode> upserts = new LinkedHashMap<>();
        Map<String, JsonNode> deletes = new LinkedHashMap<>();

        try {
            executeCompaction(records, upserts, deletes);

            if (upserts.isEmpty() && deletes.isEmpty()) {
                committer.markBatchFinished();
                LOGGER.info("EXASOL Batch completed (empty), {}", batchTimer.stopAndFormat());
                return;
            }

            Connection connection = getConnection();
            connection.setAutoCommit(false);

            executeDeletePrepared(deletes, upserts);
            executeInsertPrepared(upserts);

            NanoTimer commitTimer = NanoTimer.start();
            connection.commit();
            LOGGER.info("EXASOL Commit - {} table, {}", this.tableName, commitTimer.stopAndFormat());

        }
        catch (SQLException sqle) {
            LOGGER.error("EXASOL SQL Error: {}", sqle.getMessage(), sqle);
            try {
                if (this.conn != null)
                    this.conn.rollback();
            }
            catch (SQLException rbEx) {
                LOGGER.error("EXASOL Rollback failed: {}", rbEx.getMessage(), rbEx);
            }
            throw new InterruptedException("EXASOL batch failed due to SQL exception");
        }
        catch (Exception e) {
            LOGGER.error("EXASOL Processing error: {}", e.getMessage(), e);
            throw new InterruptedException("EXASOL batch failed due to processing exception");
        }
        finally {
            committer.markBatchFinished();
            LOGGER.info("EXASOL Batch completed - {} records, {} table, {}", records.size(), this.tableName, batchTimer.stopAndFormat());
        }
    }

    /**
     * Compacts serialized Debezium Server ChangeEvents.
     * Expects event.value() to be the Debezium envelope JSON with "payload".
     *
     * upserts/deletes maps keyed by buildEventKey(event) and storing the payload JsonNode.
     */
    public void executeCompaction(List<ChangeEvent<Object, Object>> records,
                                  Map<String, JsonNode> upserts,
                                  Map<String, JsonNode> deletes) {
        int rowsCompacted = 0;
        int rowsSkipped = 0;
        NanoTimer timer = NanoTimer.start();

        for (ChangeEvent<Object, Object> event : records) {
            try {
                // Parse payload (value)
                if (event.value() == null) {
                    rowsSkipped++;
                    continue;
                }

                JsonNode valueNode = parseJsonNode(event.value());
                JsonNode payload = valueNode.path("payload");

                if (payload.isMissingNode() || payload.isNull()) {
                    rowsSkipped++;
                    continue;
                }

                String op = payload.path("op").asText("?");

                String key = buildEventKey(event, payload);

                switch (op) {
                    case "?":
                        rowsSkipped++;
                        LOGGER.debug("EXASOL Record skipped (op='?'), key={}", key);
                        break;

                    case "d":
                        deletes.put(key, payload);
                        if (upserts.remove(key) != null)
                            rowsCompacted++;
                        break;

                    default: // c, u, r => upsert
                        upserts.put(key, payload);
                        if (deletes.remove(key) != null)
                            rowsCompacted++;
                        break;
                }
            }
            catch (Exception ex) {
                // Don't let a single broken record kill the batch; count and continue.
                rowsSkipped++;
                LOGGER.warn("EXASOL Skipping record due to parse/processing error: {}", ex.getMessage(), ex);
            }
        }

        LOGGER.info("EXASOL Compaction - {} records compacted, {} records skipped, {}", rowsCompacted, rowsSkipped, timer.stopAndFormat());
    }

    /**
     * Inserts rows from the 'after' object of each payload.
     */
    public void executeInsertPrepared(Map<String, JsonNode> upserts) throws SQLException {
        if (upserts == null || upserts.isEmpty())
            return;

        PreparedStatement stmt = null;
        StringBuilder sql = new StringBuilder();
        int rowCount = 0;

        for (Map.Entry<String, JsonNode> e : upserts.entrySet()) {
            JsonNode payload = e.getValue();
            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull() || !after.fields().hasNext()) {
                continue;
            }

            if (stmt == null) {
                this.tableName = getTableName(payload.path("source"));
                // Build INSERT INTO table VALUES (?, ?, ... ) using after's fields order
                int fields = countObjectFields(after);
                sql.append("INSERT INTO ").append(this.tableName).append(" VALUES (")
                        .append(String.join(", ", Collections.nCopies(fields, "?")))
                        .append(")");
                LOGGER.info("EXASOL SQL Statement - {}", sql.toString());
                stmt = getConnection().prepareStatement(sql.toString());
            }

            int col = 1;
            for (Iterator<Map.Entry<String, JsonNode>> it = after.fields(); it.hasNext();) {
                Map.Entry<String, JsonNode> fld = it.next();
                Object jdbcValue = toJdbcValue(fld.getValue());
                stmt.setObject(col++, jdbcValue);
            }
            stmt.addBatch();
            rowCount++;
        }

        if (stmt != null) {
            NanoTimer timer = NanoTimer.start();
            int[] result = stmt.executeBatch();
            LOGGER.info("EXASOL SQL Completed - {} rows inserted, {} rows prepared, {} table, INSERT PREPARED operation, {}",
                    result.length, rowCount, this.tableName, timer.stopAndFormat());
            stmt.close();
        }
    }

    /**
     * Deletes rows using key columns. Tries deletes first for deletes map, then for upserts map (to handle compaction order).
     * Assumes key columns are present in the payload's "before" or in the event.key().
     */
    public void executeDeletePrepared(Map<String, JsonNode> deletes, Map<String, JsonNode> upserts) throws SQLException {
        int deletesSize = (deletes == null ? 0 : deletes.size());
        int upsertsSize = (upserts == null ? 0 : upserts.size());
        if (deletesSize + upsertsSize == 0)
            return;

        PreparedStatement stmt = null;
        StringBuilder sql = new StringBuilder();
        int rowCount = 0;

        List<Map<String, JsonNode>> groups = new ArrayList<>();
        groups.add(deletes);
        groups.add(upserts);

        for (Map<String, JsonNode> group : groups) {
            if (group == null)
                continue;
            for (Map.Entry<String, JsonNode> e : group.entrySet()) {
                JsonNode payload = e.getValue();
                JsonNode before = payload.path("before");
                JsonNode source = payload.path("source");

                // Build DELETE stmt once
                if (stmt == null) {
                    this.tableName = getTableName(source);
                    // Determine key columns: attempt to use 'before' object's fields; if missing, fallback to event.key() is handled in buildEventKey
                    List<String> keyColumns = extractKeyColumns(before);
                    if (keyColumns.isEmpty()) {
                        // As last resort, attempt to use 'after' fields (not ideal) - treat first field as key
                        JsonNode after = payload.path("after");
                        if (after.isObject()) {
                            Iterator<String> it = after.fieldNames();
                            if (it.hasNext())
                                keyColumns = Collections.singletonList(it.next());
                        }
                    }
                    if (keyColumns.isEmpty()) {
                        LOGGER.warn("EXASOL Cannot determine key columns for DELETE for table {} - skipping", this.tableName);
                        return;
                    }

                    sql.append("DELETE FROM ").append(this.tableName).append(" WHERE ");
                    for (int i = 0; i < keyColumns.size(); i++) {
                        if (i > 0)
                            sql.append(" AND ");
                        sql.append(keyColumns.get(i)).append(" = ?");
                    }
                    LOGGER.info("EXASOL SQL Statement - {}", sql.toString());
                    stmt = getConnection().prepareStatement(sql.toString());
                }

                // populate key values
                List<Object> keyValues = extractKeyValues(payload);
                if (keyValues.isEmpty()) {
                    LOGGER.warn("EXASOL Skipping delete because no key values found for payload: {}", payload);
                    continue;
                }
                for (int i = 0; i < keyValues.size(); i++) {
                    stmt.setObject(i + 1, keyValues.get(i));
                }
                stmt.addBatch();
                rowCount++;
            }
        }

        if (stmt != null) {
            NanoTimer timer = NanoTimer.start();
            int[] result = stmt.executeBatch();
            LOGGER.info("EXASOL SQL Completed - {} rows deleted, {} table, DELETE PREPARED operation, {}", result.length, this.tableName, timer.stopAndFormat());
            stmt.close();
        }
    }

    // -----------------------
    // Utility helpers
    // -----------------------

    private JsonNode parseJsonNode(Object o) throws Exception {
        if (o instanceof JsonNode)
            return (JsonNode) o;
        if (o == null)
            return mapper.nullNode();
        return mapper.readTree(o.toString());
    }

    /**
     * Build a compact string key for dedup/compaction.
     * Priority:
     * 1) event.key() (if present and non-empty)
     * 2) payload.after primary fields
     * 3) payload.before primary fields
     * 4) source.db + source.table + timestamp fallback
     */
    private String buildEventKey(ChangeEvent<Object, Object> event, JsonNode payload) {
        try {
            // prefer event.key()
            if (event.key() != null) {
                JsonNode keyNode = parseJsonNode(event.key());
                if (keyNode != null && keyNode.isObject() && keyNode.size() > 0) {
                    return keyNode.toString();
                }
                else if (keyNode != null && !keyNode.isMissingNode() && !keyNode.isNull()) {
                    return keyNode.asText();
                }
            }

            // fallback to after/before fields
            JsonNode after = payload.path("after");
            if (after.isObject() && after.size() > 0) {
                return composeFieldsKey(after);
            }
            JsonNode before = payload.path("before");
            if (before.isObject() && before.size() > 0) {
                return composeFieldsKey(before);
            }

            // last resort: source db.table + ts
            JsonNode source = payload.path("source");
            String db = source.path("db").asText("unknown_db");
            String table = source.path("table").asText("unknown_table");
            String ts = payload.path("ts_ms").asText(Long.toString(System.currentTimeMillis()));
            return db + "." + table + "#" + ts;
        }
        catch (Exception e) {
            LOGGER.warn("Failed building key from event: {}", e.getMessage(), e);
            return UUID.randomUUID().toString();
        }
    }

    private String composeFieldsKey(JsonNode obj) {
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> en = it.next();
            sb.append(en.getKey()).append("=").append(en.getValue().asText()).append(";");
        }
        return sb.toString();
    }

    private List<String> extractKeyColumns(JsonNode before) {
        if (before == null || !before.isObject())
            return Collections.emptyList();
        List<String> cols = new ArrayList<>();
        for (Iterator<String> it = before.fieldNames(); it.hasNext();) {
            cols.add(it.next());
        }
        return cols;
    }

    /**
     * Extract key values (attempt to use before->fields, else after->fields, else event.key).
     */
    private List<Object> extractKeyValues(JsonNode payload) {
        List<Object> keyValues = new ArrayList<>();
        JsonNode before = payload.path("before");
        JsonNode after = payload.path("after");
        if (before.isObject() && before.size() > 0) {
            for (Iterator<Map.Entry<String, JsonNode>> it = before.fields(); it.hasNext();) {
                keyValues.add(toJdbcValue(it.next().getValue()));
            }
        }
        else if (after.isObject() && after.size() > 0) {
            // If no before, try after fields (may not be primary key)
            for (Iterator<Map.Entry<String, JsonNode>> it = after.fields(); it.hasNext();) {
                keyValues.add(toJdbcValue(it.next().getValue()));
            }
        }
        return keyValues;
    }

    private String getTableName(JsonNode source) {
        if (source != null && source.isObject()) {
            String db = source.path("db").asText(null);
            String table = source.path("table").asText(null);
            if (db != null && table != null) {
                return db + "." + table;
            }
        }
        return "unknown_table";
    }

    /**
     * Convert JsonNode to a reasonable JDBC value.
     * Extend this method to support Debezium logical types precisely (dates/timestamps/micros).
     */
    private Object toJdbcValue(JsonNode node) {
        if (node == null || node.isNull())
            return null;
        if (node.isNumber()) {
            // prefer Long for integer-like, Double otherwise
            if (node.isIntegralNumber()) {
                return node.longValue();
            }
            return node.doubleValue();
        }
        if (node.isBoolean())
            return node.booleanValue();
        if (node.isTextual())
            return node.asText();
        // fallback to JSON string representation for complex types
        return node.toString();
    }

    private int countObjectFields(JsonNode node) {
        if (node == null || !node.isObject())
            return 0;
        int c = 0;
        for (Iterator<?> it = node.fieldNames(); it.hasNext(); it.next())
            c++;
        return c;
    }

    private Connection getConnection() throws SQLException {
        if (this.conn == null || this.conn.isClosed()) {
            if (this.url == null || this.username == null) {
                throw new SQLException("EXASOL connection properties not provided");
            }
            this.conn = DriverManager.getConnection(this.url, this.username, this.password);
            LOGGER.info("EXASOL Connected to {}", this.url);
        }
        return this.conn;
    }

    public void closeConnection() {
        try {
            if (this.conn != null && !this.conn.isClosed()) {
                this.conn.close();
                LOGGER.info("EXASOL Disconnected");
            }
            this.conn = null;
        }
        catch (SQLException e) {
            LOGGER.warn("Error closing connection: {}", e.getMessage(), e);
        }
    }

    // Lightweight timer replacement for your previous Stopwatch usage
    private static class NanoTimer {
        private final long start;

        private NanoTimer() {
            this.start = System.nanoTime();
        }

        static NanoTimer start() {
            return new NanoTimer();
        }

        String stopAndFormat() {
            long elapsedNanos = System.nanoTime() - start;
            return formatNanos(elapsedNanos);
        }

        static String formatNanos(long nanos) {
            long ms = nanos / 1_000_000L;
            return ms + " ms";
        }
    }
}
