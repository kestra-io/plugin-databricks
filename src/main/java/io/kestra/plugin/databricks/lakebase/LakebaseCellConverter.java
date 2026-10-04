package io.kestra.plugin.databricks.lakebase;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.UUID;

/**
 * Maps Lakebase / Postgres JDBC cell values into types Kestra can serialize.
 */
final class LakebaseCellConverter {
    private final ZoneId zoneId;

    LakebaseCellConverter(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        Object data = rs.getObject(columnIndex);
        if (data == null) {
            return null;
        }

        if (data instanceof Date date) {
            return date.toLocalDate();
        }
        if (data instanceof Time time) {
            return time.toLocalTime();
        }
        if (data instanceof Timestamp timestamp) {
            return timestamp.toInstant().atZone(zoneId);
        }
        if (data instanceof UUID uuid) {
            return uuid.toString();
        }
        if (data instanceof BigDecimal bigDecimal) {
            return bigDecimal;
        }
        if (data instanceof Array array) {
            Object raw = array.getArray();
            if (raw instanceof Object[] objects) {
                return Arrays.asList(objects);
            }
            return raw;
        }
        if (data instanceof byte[]) {
            return data;
        }

        // Postgres JSON/JSONB and other extension types typically arrive as PGobject
        String className = data.getClass().getName();
        if ("org.postgresql.util.PGobject".equals(className)) {
            try {
                return data.getClass().getMethod("getValue").invoke(data);
            } catch (ReflectiveOperationException e) {
                return data.toString();
            }
        }

        return data;
    }
}
