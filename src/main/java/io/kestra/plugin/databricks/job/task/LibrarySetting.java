package io.kestra.plugin.databricks.job.task;

import java.util.List;

import com.databricks.sdk.service.compute.Library;
import com.databricks.sdk.service.compute.MavenLibrary;
import com.databricks.sdk.service.compute.PythonPyPiLibrary;
import com.databricks.sdk.service.compute.RCranLibrary;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@Schema(
    title = "Library to install on the cluster",
    description = "Set exactly one of the library types (`cran`, `egg`, `jar`, `maven`, `pypi`, or `whl`)."
)
public class LibrarySetting {
    @PluginProperty(dynamic = true, group = "advanced")
    @Schema(title = "CRAN library", description = "An R package to install from a CRAN repository.")
    private CranSetting cran;

    @Schema(title = "Egg library", description = "URI of a Python egg to install (for example a DBFS or cloud storage path).")
    private Property<String> egg;

    @Schema(title = "JAR library", description = "URI of a JAR to install (for example a DBFS or cloud storage path).")
    private Property<String> jar;

    @PluginProperty(dynamic = true, group = "advanced")
    @Schema(title = "Maven library", description = "A Maven artifact to install on the cluster.")
    private MavenSetting maven;

    @PluginProperty(dynamic = true, group = "advanced")
    @Schema(title = "PyPI library", description = "A Python package to install from a PyPI repository.")
    private PypiSetting pypi;

    @Schema(title = "Wheel library", description = "URI of a Python wheel (.whl) to install (for example a DBFS or cloud storage path).")
    private Property<String> whl;

    public Library toLibrary(RunContext runContext) throws IllegalVariableEvaluationException {
        return new Library()
            .setCran(cran != null ? cran.toCran(runContext) : null)
            .setEgg(runContext.render(egg).as(String.class).orElse(null))
            .setJar(runContext.render(jar).as(String.class).orElse(null))
            .setMaven(maven != null ? maven.toMaven(runContext) : null)
            .setPypi(pypi != null ? pypi.toPypi(runContext) : null)
            .setWhl(runContext.render(whl).as(String.class).orElse(null));
    }

    @Builder
    @Getter
    public static class CranSetting {
        @Schema(title = "Package name", description = "Name of the CRAN package to install.")
        private Property<String> _package;

        @Schema(title = "Repository", description = "CRAN repository URL to install the package from; defaults to the Databricks default repository.")
        private Property<String> repo;

        public RCranLibrary toCran(RunContext runContext) throws IllegalVariableEvaluationException {
            return new RCranLibrary()
                .setPackage(runContext.render(_package).as(String.class).orElse(null))
                .setRepo(runContext.render(repo).as(String.class).orElse(null));
        }
    }

    @Builder
    @Getter
    public static class MavenSetting {
        @Schema(title = "Coordinates", description = "Gradle-style Maven coordinates, for example `org.jsoup:jsoup:1.7.2`.")
        private Property<String> coordinates;

        @Schema(title = "Repository", description = "Maven repository URL to install the artifact from; defaults to Maven Central.")
        private Property<String> repo;

        @Schema(title = "Exclusions", description = "List of dependencies to exclude, for example `slf4j:slf4j`.")
        private Property<List<String>> exclusions;

        public MavenLibrary toMaven(RunContext runContext) throws IllegalVariableEvaluationException {
            return new MavenLibrary()
                .setCoordinates(runContext.render(coordinates).as(String.class).orElse(null))
                .setExclusions(runContext.render(exclusions).asList(String.class).isEmpty() ? null : runContext.render(exclusions).asList(String.class))
                .setRepo(runContext.render(repo).as(String.class).orElse(null));
        }
    }

    @Builder
    @Getter
    public static class PypiSetting {
        @Schema(title = "Package name", description = "Name of the PyPI package to install, optionally pinned (for example `simplejson==3.8.0`).")
        private Property<String> _package;

        @Schema(title = "Repository", description = "PyPI repository URL to install the package from; defaults to the public PyPI index.")
        private Property<String> repo;

        public PythonPyPiLibrary toPypi(RunContext runContext) throws IllegalVariableEvaluationException {
            return new PythonPyPiLibrary()
                .setPackage(runContext.render(_package).as(String.class).orElse(null))
                .setRepo(runContext.render(repo).as(String.class).orElse(null));
        }
    }
}
