package io.kestra.plugin.databricks;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.databricks.cluster.CreateCluster;
import io.kestra.plugin.databricks.sql.Query;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyGroupTest {
    @Test
    void groupsRelatedPropertiesTogether() throws NoSuchFieldException {
        assertEquals("advanced", groupFor(CreateCluster.class, "autoTerminationMinutes"));
        assertEquals("connection", groupFor(Query.class, "catalog"));
        assertEquals("connection", groupFor(Query.class, "schema"));
    }

    private static String groupFor(Class<?> type, String fieldName) throws NoSuchFieldException {
        return type.getDeclaredField(fieldName).getAnnotation(PluginProperty.class).group();
    }
}
