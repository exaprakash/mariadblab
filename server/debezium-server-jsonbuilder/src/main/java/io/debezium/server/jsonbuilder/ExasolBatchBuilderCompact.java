package io.debezium.server.jsonbuilder;
import com.fasterxml.jackson.databind.*;
import io.burt.jmespath.Expression;
import io.burt.jmespath.jackson.JacksonRuntime;
import io.debezium.engine.ChangeEvent;

import java.sql.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class ExasolBatchBuilderCompact {

    private static final ObjectMapper M = new ObjectMapper();
    private static final JacksonRuntime R = new JacksonRuntime();
    private static final Expression<JsonNode> OP = R.compile("Value.payload.op");
    private static final Expression<JsonNode> KEY = R.compile("Key.payload");
    private static final Expression<JsonNode> VALFIELDS = R.compile("Value.schema.fields[?field == 'after'].fields[].field");
    private static final Expression<JsonNode> VALDATA = R.compile("Value.payload.after");
    private static final Expression<JsonNode> DB = R.compile("Value.payload.source.db");
    private static final Expression<JsonNode> TBL = R.compile("Value.payload.source.table");

    public record StatementBatch(PreparedStatement deletes, PreparedStatement inserts) {}

    public static StatementBatch build(Connection conn, List<ChangeEvent<Object, Object>> records) throws Exception {
        if (records == null || records.isEmpty()) return new StatementBatch(null, null);

        // Compact and partition in one pass
        Map<Boolean, List<JsonNode>> partitioned =
                records.stream()
                        .map(ExasolBatchBuilderCompact::toJson)                         // ChangeEvent → JsonNode
                        .filter(Objects::nonNull)
                        .filter(e -> KEY.search(e) != null && !KEY.search(e).isNull())
                        .collect(Collectors.toMap(                                        // compact inline
                                e -> KEY.search(e).toString(),
                                Function.identity(),
                                (oldE, newE) -> newer(oldE, newE),
                                LinkedHashMap::new
                        ))
                        .values().stream()
                        .collect(Collectors.partitioningBy(e ->
                                "d".equalsIgnoreCase(OP.search(e).asText())
                        ));

        List<JsonNode> dels = partitioned.get(true);
        List<JsonNode> ups = partitioned.get(false);
        if (dels.isEmpty() && ups.isEmpty()) return new StatementBatch(null, null);

        JsonNode ref = !ups.isEmpty() ? ups.get(0) : dels.get(0);
        String table = DB.search(ref).asText() + "." + TBL.search(ref).asText();

        return new StatementBatch(
                makeDelete(conn, table, dels),
                makeInsert(conn, table, ups)
        );
    }

    private static JsonNode toJson(ChangeEvent<Object,Object> ev) {
        try {
            ObjectNode node = M.createObjectNode();
            node.set("Key", M.readTree(String.valueOf(ev.key())));
            node.set("Value", M.readTree(String.valueOf(ev.value())));
            return node;
        } catch (Exception e) {
            return null;
        }
    }

    private static JsonNode newer(JsonNode oldE, JsonNode newE) {
        String op = Optional.ofNullable(OP.search(newE)).map(JsonNode::asText).orElse("?");
        if ("d".equalsIgnoreCase(op) || Set.of("c", "u", "r").contains(op)) return newE;
        return oldE;
    }

    private static PreparedStatement makeDelete(Connection c, String t, List<JsonNode> evs) throws SQLException {
        if (evs.isEmpty()) return null;
        List<String> cols = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(KEY.search(evs.get(0)).fieldNames(), 0), false
        ).toList();
        String sql = "DELETE FROM " + t + " WHERE " + String.join(" AND ", cols.stream().map(x -> x + "=?").toList());
        PreparedStatement ps = c.prepareStatement(sql);
        evs.forEach(e -> bind(ps, KEY.search(e), cols));
        return ps;
    }

    private static PreparedStatement makeInsert(Connection c, String t, List<JsonNode> evs) throws SQLException {
        if (evs.isEmpty()) return null;
        List<String> cols = StreamSupport.stream(VALFIELDS.search(evs.get(0)).spliterator(), false)
                .map(JsonNode::asText).toList();
        String sql = "INSERT INTO " + t + " (" + String.join(", ", cols) + ") VALUES (" +
                String.join(", ", Collections.nCopies(cols.size(), "?")) + ")";
        PreparedStatement ps = c.prepareStatement(sql);
        evs.forEach(e -> bind(ps, VALDATA.search(e), cols));
        return ps;
    }

    private static void bind(PreparedStatement ps, JsonNode row, List<String> cols) {
        try {
            for (int i = 0; i < cols.size(); i++)
                ps.setObject(i + 1, jval(row.get(cols.get(i))));
            ps.addBatch();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    private static Object jval(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (n.isNumber()) return n.numberValue();
        if (n.isTextual()) return n.asText();
        return n.toString();
    }
}
