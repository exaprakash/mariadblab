/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.console;

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
@Named("console")
@Dependent
public class ConsoleChangeConsumer extends BaseChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleChangeConsumer.class);
    public static final String PROP_PREFIX = "debezium.sink.console.";

    @PostConstruct
    void connect() throws URISyntaxException {
        // System.err.println(">>> ConsoleChangeConsumer CONSTRUCTOR CALLED <<<");
        final Config config = ConfigProvider.getConfig();
        initWithConfig(config);
        LOGGER.info(">>> ConsoleChangeConsumer @PostConstruct triggered");
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
        // Create or reuse a file appender for console sink logging
        Path logFile = Paths.get("logs/cdc-console-consumer.json");
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

        try (BufferedWriter writer = Files.newBufferedWriter(logFile, StandardOpenOption.APPEND)) {
            for (ChangeEvent<Object, Object> record : records) {
                Object key = record.key();
                Object value = record.value();

                UUID messageId = UUID.randomUUID();
                Instant now = Instant.now();

                String formattedMessage = String.format(
                        "[%s] [MessageID: %s] Key=%s, Value=%s",
                        now,
                        messageId,
                        key != null ? key.toString() : "null",
                        value != null ? value.toString() : "null");

                // Write to file
                writer.write(formattedMessage);
                writer.newLine();

                // Also log via SLF4J
                LOGGER.info("ConsoleChangeConsumer::Received event {}", formattedMessage);

                // Mark as processed
                if (value != null) {
                    committer.markProcessed(record);
                }
            }

            // Finish the batch
            committer.markBatchFinished();
            writer.flush();
            LOGGER.debug("ConsoleChangeConsumer::Completed batch of {} records", records.size());
        }
        catch (IOException e) {
            LOGGER.error("Error writing change events to log file", e);
        }
        catch (Exception e) {
            LOGGER.error("Unexpected error processing CDC batch", e);
        }
    }

}
