package com.exasol.debezium.engine.sink;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.util.Stopwatch;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExasolChangeConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {
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

    public ExasolChangeConsumer(Properties props) {
        this.conn = null;
        this.url = props.get("exasol.connection.url").toString();
        this.username = props.get("exasol.connection.username").toString();
        this.password = props.get("exasol.connection.password").toString();
    }

    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records, DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        int hour = (short) (int) System.currentTimeMillis() / 3600000;
        if (hour != this.currentHour) {
            this.currentHour = hour;
            this.currentHourRecordsIn = records.size();
        } else {
            this.currentHourRecordsIn += records.size();
        }
        LOGGER.info("EXASOL Batch received - {} records, {} current hour records",
                Integer.valueOf(records.size()), Long.valueOf(this.currentHourRecordsIn));
        Stopwatch timer = Stopwatch.reusable();
        timer.start();
        Map<SourceRecordKey, SourceRecord> upserts = new LinkedHashMap<>();
        Map<SourceRecordKey, SourceRecord> deletes = new LinkedHashMap<>();
        executeCompaction(records, upserts, deletes);
        if (upserts.isEmpty() && deletes.isEmpty()) {
            committer.markBatchFinished();
            timer.stop();
            LOGGER.info("EXASOL Batch completed (empty), {}", timer.durations());
            return;
        }
        try {
            getConnection().setAutoCommit(false);
            executeDeletePrepared(deletes, upserts);
            executeInsertPrepared(upserts);
            Stopwatch commitTimer = Stopwatch.reusable();
            commitTimer.start();
            this.conn.commit();
            commitTimer.stop();
            LOGGER.info("EXASOL Commit - {} table, {}", this.tableName, commitTimer.durations());
        } catch (SQLException e) {
            LOGGER.error("EXASOL Error: {}", e.getMessage(), e);
            try {
                if (this.conn != null)
                    this.conn.rollback();
            } catch (SQLException sQLException) {
            }
            throw new InterruptedException();
        }
        committer.markBatchFinished();
        timer.stop();
        LOGGER.info("EXASOL Batch completed - {} records, {} table, {}", new Object[]{Integer.valueOf(records.size()), this.tableName, timer
                .durations()});
    }

    public void executeCompaction(List<RecordChangeEvent<SourceRecord>> records, Map<SourceRecordKey, SourceRecord> upserts, Map<SourceRecordKey, SourceRecord> deletes) {
        int rowsCompacted = 0;
        int rowsSkipped = 0;
        Stopwatch timer = Stopwatch.reusable();
        timer.start();
        for (RecordChangeEvent<SourceRecord> record : records) {
            SourceRecord sourceRecord = (SourceRecord) record.record();
            LOGGER.debug("EXASOL Record - {}", sourceRecord.toString());
            SourceRecordKey keyValues = new SourceRecordKey(sourceRecord);
            String op = keyValues.getOp();
            if ("?".equals(op)) {
                LOGGER.debug("EXASOL Record skipped");
                rowsSkipped++;
                continue;
            }
            if ("d".equals(op)) {
                deletes.put(keyValues, sourceRecord);
                if (upserts.remove(keyValues) != null)
                    rowsCompacted++;
                continue;
            }
            upserts.put(keyValues, sourceRecord);
            if (deletes.remove(keyValues) != null)
                rowsCompacted++;
        }
        timer.stop();
        LOGGER.info("EXASOL Compaction - {} records compacted, {} records skipped, {}", new Object[]{Integer.valueOf(rowsCompacted), Integer.valueOf(rowsSkipped), timer.durations()});
    }

    public void executeInsertPrepared(Map<SourceRecordKey, SourceRecord> upserts) throws SQLException {
        if (upserts == null || upserts.isEmpty())
            return;
        PreparedStatement stmt = null;
        StringBuilder builder = new StringBuilder();
        int row = 0;
        for (Map.Entry<SourceRecordKey, SourceRecord> e : upserts.entrySet()) {
            SourceRecord record = e.getValue();
            Struct valueStruct = (Struct) record.value();
            Struct afterStruct = valueStruct.getStruct("after");
            if (stmt == null) {
                this.tableName = getTableName(valueStruct);
                builder.append("INSERT INTO ");
                builder.append(this.tableName);
                builder.append(" VALUES (");
                int i = 0;
                for (Field field : afterStruct.schema().fields()) {
                    if (i > 0)
                        builder.append(", ");
                    builder.append("?");
                    i++;
                }
                builder.append(")");
                LOGGER.info("EXASOL SQL Statement - {}", builder.toString());
                stmt = getConnection().prepareStatement(builder.toString());
            }
            int cnt = 0;
            for (Field field : afterStruct.schema().fields()) {
                String name = field.name();
                Schema fieldSchema = afterStruct.schema().field(name).schema();
                Object fieldValue = afterStruct.get(field.name());
                Object value = getJdbcValue(fieldValue, field, fieldSchema);
                stmt.setObject(cnt + 1, value);
                cnt++;
            }
            stmt.addBatch();
            row++;
        }
        if (stmt != null) {
            Stopwatch timer = Stopwatch.reusable();
            timer.start();
            int[] batchResult = stmt.executeBatch();
            timer.stop();
            LOGGER.info("EXASOL SQL Completed - {} rows inserted, {} rows prepared, {} table, INSERT PREPARED operation, {}", new Object[]{Integer.valueOf(batchResult.length), Integer.valueOf(row), this.tableName, timer.durations()});
            stmt.close();
        }
    }

    public void executeDeletePrepared(Map<SourceRecordKey, SourceRecord> deletes, Map<SourceRecordKey, SourceRecord> upserts) throws SQLException {
        if (deletes == null || (deletes.isEmpty() && upserts == null) || deletes.size() + upserts.size() == 0)
            return;
        Map[] arrayOfMap = {deletes, upserts};
        PreparedStatement stmt = null;
        StringBuilder builder = new StringBuilder();
        int row = 0;
        for (Map<SourceRecordKey, SourceRecord> a : arrayOfMap) {
            if (a != null)
                for (Map.Entry<SourceRecordKey, SourceRecord> e : a.entrySet()) {
                    SourceRecord record = e.getValue();
                    Struct valueStruct = (Struct) record.value();
                    Struct afterStruct = (valueStruct.schema().field("after") != null) ? valueStruct.getStruct("after") : null;
                    if (stmt == null) {
                        this.tableName = getTableName(valueStruct);
                        builder.append("DELETE FROM ");
                        builder.append(this.tableName);
                        builder.append(" WHERE ");
                        int i = 0;
                        for (Field field : record.keySchema().fields()) {
                            if (i > 0)
                                builder.append(" AND ");
                            builder.append(field.name());
                            builder.append(" = ?");
                            i++;
                        }
                        LOGGER.info("EXASOL SQL Statement - {}", builder.toString());
                        stmt = getConnection().prepareStatement(builder.toString());
                    }
                    int cnt = 0;
                    Struct keyStruct = (Struct) record.key();
                    for (Field field : record.keySchema().fields()) {
                        String name = field.name();
                        Schema fieldSchema = (afterStruct != null) ? afterStruct.schema().field(name).schema() : null;
                        Object value = getJdbcValue(keyStruct.get(field.name()), field, fieldSchema);
                        stmt.setObject(cnt + 1, value);
                        cnt++;
                    }
                    stmt.addBatch();
                    row++;
                }
        }
        if (stmt != null) {
            Stopwatch timer = Stopwatch.reusable();
            timer.start();
            int[] batchResult = stmt.executeBatch();
            timer.stop();
            LOGGER.info("EXASOL SQL Completed - {} rows deleted, {} table, DELETE PREPARED operation, {}", new Object[]{Integer.valueOf(batchResult.length), this.tableName, timer.durations()});
            stmt.close();
        }
    }

    public String getTableName(Struct valueStruct) {
        if (valueStruct != null && valueStruct.schema().field("source") != null) {
            Struct sourceStruct = valueStruct.getStruct("source");
            return sourceStruct.getString("db") + "." + sourceStruct.getString("table");
        }
        return "";
    }

    public Object getJdbcValue(Object o, Field field, Schema fieldSchema) {
        if (o == null || field == null)
            return null;
        if (fieldSchema == null)
            return o;
        String fieldType = field.schema().type().toString();
        String fieldTypeDebezium = fieldSchema.name();
        LOGGER.debug("EXASOL Field - {}, {}, {}", new Object[]{field.name(), fieldType, fieldTypeDebezium});
        if (fieldTypeDebezium == null)
            return o;
        if (o instanceof Number && fieldTypeDebezium.equalsIgnoreCase("io.debezium.time.Date"))
            return Date.valueOf(LocalDate.ofEpochDay(((Number) o).longValue()));
        if (o instanceof Number && fieldTypeDebezium.equalsIgnoreCase("io.debezium.time.Timestamp"))
            return Timestamp.valueOf(LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) o).longValue()), ZoneOffset.UTC));
        if (o instanceof Number && fieldTypeDebezium.equalsIgnoreCase("io.debezium.time.MicroTimestamp")) {
            long micros = ((Number) o).longValue();
            Instant instant = Instant.ofEpochSecond(TimeUnit.MICROSECONDS
                    .toSeconds(micros), TimeUnit.MICROSECONDS
                    .toNanos(micros % TimeUnit.SECONDS.toMicros(1L)));
            return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
        }
        return o;
    }

    private Connection getConnection() throws SQLException {
        if (this.conn == null) {
            this.conn = DriverManager.getConnection(this.url, this.username, this.password);
            LOGGER.info("EXASOL Connected");
        }
        return this.conn;
    }

    public void closeConnection() {
        try {
            if (this.conn != null) {
                this.conn.close();
                LOGGER.info("EXASOL Disconnected");
            }
            this.conn = null;
        } catch (SQLException sQLException) {
        }
    }
}
