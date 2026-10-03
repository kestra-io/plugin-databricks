package io.kestra.plugin.databricks.unitycatalog;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.catalog.CatalogInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnityCatalogUtilsTest {
    @Test
    void securableTypeIsLowerCased() {
        assertThat(UnityCatalogUtils.securableType("SCHEMA"), is("schema"));
        assertThat(UnityCatalogUtils.securableType(" External Location "), is("external_location"));
    }

    @Test
    void validVolumePath() {
        assertDoesNotThrow(() -> UnityCatalogUtils.validateVolumePath("/Volumes/main/landing_zone/raw_files/data.csv"));
        assertDoesNotThrow(() -> UnityCatalogUtils.validateVolumePath("/Volumes/main/landing_zone/raw_files/dir/data.csv"));
    }

    @Test
    void invalidVolumePath() {
        for (var path : new String[] { "/Share/data.csv", "/Volumes/main/landing_zone/raw_files", "Volumes/main/s/v/f.csv", "/Volumes/main/s" }) {
            var exception = assertThrows(IllegalArgumentException.class, () -> UnityCatalogUtils.validateVolumePath(path));
            assertThat(exception.getMessage(), containsString(path));
        }
    }

    @Test
    void toMapsHandlesNull() {
        assertThat(UnityCatalogUtils.toMaps(null).isEmpty(), is(true));
    }

    @Test
    void toMapSerializesSdkModels() {
        var map = UnityCatalogUtils.toMap(new CatalogInfo().setName("main").setOwner("me"));

        assertThat(map.get("name"), is("main"));
        assertThat(map.get("owner"), is("me"));
    }
}
