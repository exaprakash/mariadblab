
package io.debezium.server.jsonbuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.engine.ChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ChangeEventJsonBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEventJsonBuilder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static List<JsonNode> buildJsonNodes(List<ChangeEvent<Object, Object>> records) {
        List<JsonNode> result = new ArrayList<>();

        for (ChangeEvent<Object, Object> event : records) {
            try {
                // Get key and value JSON strings (Debezium gives them as serialized JSON)
                String keyJson = event.key() != null ? event.key().toString() : "{}";
                String valueJson = event.value() != null ? event.value().toString() : "{}";

                // Parse them into JsonNodes
                JsonNode keyNode = MAPPER.readTree(keyJson);
                JsonNode valueNode = MAPPER.readTree(valueJson);

                // Create combined object node
                ObjectNode combined = MAPPER.createObjectNode();
                combined.set("Key", keyNode);
                combined.set("Value", valueNode);

                result.add(combined);

            } catch (Exception e) {
                LOGGER.error("Failed to parse ChangeEvent as JSON: {}", e.getMessage(), e);
            }
        }

        return result;
    }
}
