package io.kestra.plugin.databricks.unitycatalog;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kestra.core.serializers.JacksonMapper;

/**
 * Helpers shared by the Unity Catalog tasks.
 */
public final class UnityCatalogUtils {
    private static final String VOLUMES_PREFIX = "/Volumes/";

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private UnityCatalogUtils() {
    }

    /**
     * Converts a Databricks SDK model (catalog, schema, table, volume, grant ...) to a plain map so it can be exposed as a task output.
     */
    public static Map<String, Object> toMap(Object sdkModel) {
        return JacksonMapper.ofJson().convertValue(sdkModel, MAP_TYPE);
    }

    public static List<Map<String, Object>> toMaps(Iterable<?> sdkModels) {
        if (sdkModels == null) {
            return List.of();
        }

        try (Stream<?> stream = StreamSupport.stream(sdkModels.spliterator(), false)) {
            return stream.map(UnityCatalogUtils::toMap).toList();
        }
    }

    /**
     * The Grants API expects the securable type in lower case (e.g. {@code schema}); accept any case from the user.
     */
    public static String securableType(String value) {
        return value.trim().toLowerCase(Locale.ROOT).replace(' ', '_');
    }

    /**
     * Files API paths must point inside a volume: {@code /Volumes/<catalog>/<schema>/<volume>/<path>}.
     */
    public static void validateVolumePath(String volumePath) {
        if (!volumePath.startsWith(VOLUMES_PREFIX) || volumePath.substring(VOLUMES_PREFIX.length()).split("/").length < 4) {
            throw new IllegalArgumentException(
                "Invalid volume path '" + volumePath + "', expected a file path in the form '/Volumes/<catalog>/<schema>/<volume>/<path>'"
            );
        }
    }
}
