package io.kestra.plugin.databricks.sql;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;

class DatabricksCellConverter  extends AbstractCellConverter {
    public DatabricksCellConverter(ZoneId zoneId) {
        super(zoneId);
    }

    @SneakyThrows
    @Override
    public Object convertCell(int columnIndex, ResultSet rs, Connection connection) throws SQLException {
        return super.convert(columnIndex, rs);
    }
}
