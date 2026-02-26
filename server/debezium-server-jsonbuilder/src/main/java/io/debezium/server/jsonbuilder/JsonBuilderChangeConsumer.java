/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.jsonbuilder;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.Expression;
import io.burt.jmespath.jackson.JacksonRuntime;


import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;

import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.VisibleForTesting;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.server.BaseChangeConsumer;

/**
 * Implementation of the consumer that delivers the messages to console.
 *
 * @author Prakash Subramanian
 */
@Named("jsonbuilder")
@Dependent
public class JsonBuilderChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonBuilderChangeConsumer.class);
    public static final String PROP_PREFIX = "debezium.sink.console.";
    public static final JmesPath<JsonNode> jmespath = new JacksonRuntime();
    public static final Expression<JsonNode> KEYPAYLOAD = jmespath.compile("Key.payload");
    public static final Expression<JsonNode> VALUEFIELDS = jmespath.compile("Value.schema.fields[?field == 'after'].fields[].field");
    public static final Expression<JsonNode> VALUEDATA = jmespath.compile("Value.payload.after");
    public static final Expression<JsonNode> OPDATA = jmespath.compile("Value.payload.op");


    @PostConstruct
    void connect() throws URISyntaxException {
        // System.err.println(">>> JsonBuilderChangeConsumer CONSTRUCTOR CALLED <<<");
        final Config config = ConfigProvider.getConfig();
        initWithConfig(config);
        LOGGER.info(">>> JsonBuilderChangeConsumer @PostConstruct triggered");
    }

    @VisibleForTesting
    void initWithConfig(Config config) throws URISyntaxException {
        // System.out.println("initWithConfig invoked ");
        LOGGER.trace("initWithConfig invoked ");
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records,
                            DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer)
            throws InterruptedException {
        List<JsonNode> events = ChangeEventJsonBuilder.buildJsonNodes(records);
        Map<String, JsonNode> upserts = new LinkedHashMap<>();
        Map<String, JsonNode> deletes = new LinkedHashMap<>();
        executeCompaction(events, upserts, deletes);

        for (JsonNode event : events) {
            JsonNode keyData = KEYPAYLOAD.search(event);
            JsonNode fieldNameData = VALUEFIELDS.search(event);
            JsonNode fieldValueData = VALUEDATA.search(event);
            JsonNode opNode = OPDATA.search(event);

            // Convert operation node to string value
            String opData = (opNode != null && !opNode.isNull()) ? opNode.asText() : "unknown";

            String formattedMessage = String.format(
                    "Op=%s, Key=%s, Fields=%s, Data=%s",
                    opData,
                    keyData != null ? keyData.toString() : "null",
                    fieldNameData != null ? fieldNameData.toString() : "null",
                    fieldValueData != null ? fieldValueData.toString() : "null"
            );

            LOGGER.info("JsonBuilderChangeConsumer::Received event {}", formattedMessage);
        }

        for (ChangeEvent<Object, Object> record : records) {
            // Mark as processed
//            if (value != null) {
                committer.markProcessed(record);
//            }
        }
        // Finish the batch
        committer.markBatchFinished();
        LOGGER.debug("JsonBuilderChangeConsumer::Completed batch of {} records", records.size());
    }

    public void executeCompaction(
            List<JsonNode> events,
            Map<String, JsonNode> upserts,
            Map<String, JsonNode> deletes) {

        int rowsCompacted = 0;
        int rowsSkipped = 0;
        Stopwatch timer = Stopwatch.reusable();
        timer.start();

        for (JsonNode event : events) {
            try {
                JsonNode keyNode = KEYPAYLOAD.search(event);
                JsonNode opNode = OPDATA.search(event);
                String op = (opNode != null && !opNode.isNull()) ? opNode.asText() : "?";

                if (keyNode == null || keyNode.isNull() || op.equals("?")) {
                    LOGGER.debug("Skipping event: missing key or unknown op");
                    rowsSkipped++;
                    continue;
                }

                String keyJson = keyNode.toString();

                switch (op) {
                    case "d":
                        deletes.put(keyJson, event);
                        if (upserts.remove(keyJson) != null) {
                            rowsCompacted++;
                        }
                        break;

                    case "c":
                    case "u":
                    case "r":
                        upserts.put(keyJson, event);
                        if (deletes.remove(keyJson) != null) {
                            rowsCompacted++;
                        }
                        break;

                    default:
                        LOGGER.debug("Unknown op {} — skipped event: {}", op, event);
                        rowsSkipped++;
                        break;
                }

            } catch (Exception e) {
                LOGGER.warn("Failed to compact event: {}", event, e);
                rowsSkipped++;
            }
        }

        timer.stop();
        LOGGER.info("EXASOL Compaction - {} records compacted, {} records skipped, {} total processed, {}",
                rowsCompacted, rowsSkipped, events.size(), timer.durations());
    }

}
