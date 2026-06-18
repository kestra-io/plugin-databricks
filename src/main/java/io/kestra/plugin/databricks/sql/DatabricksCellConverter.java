package io.kestra.plugin.databricks.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

import lombok.SneakyThrows;

class DatabricksCellConverter extends AbstractCellConverter {
    public DatabricksCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        return super.convert(columnIndex, rs);
    }
}
