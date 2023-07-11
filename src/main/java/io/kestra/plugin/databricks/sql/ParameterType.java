package io.kestra.plugin.databricks.sql;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

//FIXME duplicated from io.kestra.plugin.jdbc.AbstractJdbcBatch$ParameterType
class ParameterType {
    private final Map<Integer, Class<?>> cls = new HashMap<>();
    private final Map<Integer, Integer> types = new HashMap<>();
    private final Map<Integer, String> typesName = new HashMap<>();

    public static ParameterType of(ParameterMetaData parameterMetaData) throws SQLException, ClassNotFoundException {
        ParameterType parameterType = new ParameterType();

        for (int i = 1; i <= parameterMetaData.getParameterCount(); i++) {
            parameterType.cls.put(i, Class.forName(parameterMetaData.getParameterClassName(i)));
            parameterType.types.put(i, parameterMetaData.getParameterType(i));
            parameterType.typesName.put(i, parameterMetaData.getParameterTypeName(i));
        }

        return parameterType;
    }

    public Class<?> getClass(int index) {
        return this.cls.get(index);
    }

    public Integer getType(int index) {
        return this.types.get(index);
    }

    public String getTypeName(int index) {
        return this.typesName.get(index);
    }
}
