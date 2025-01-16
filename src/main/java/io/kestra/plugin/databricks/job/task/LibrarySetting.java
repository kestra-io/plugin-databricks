package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.compute.Library;
import com.databricks.sdk.service.compute.MavenLibrary;
import com.databricks.sdk.service.compute.PythonPyPiLibrary;
import com.databricks.sdk.service.compute.RCranLibrary;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class LibrarySetting {
    @PluginProperty(dynamic = true)
    private CranSetting cran;

    private Property<String> egg;

    private Property<String> jar;

    @PluginProperty(dynamic = true)
    private MavenSetting maven;

    @PluginProperty(dynamic = true)
    private PypiSetting pypi;

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
        private Property<String> _package;

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
        private Property<String> coordinates;

        private Property<String> repo;

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
        private Property<String> _package;

        private Property<String> repo;

        public PythonPyPiLibrary toPypi(RunContext runContext) throws IllegalVariableEvaluationException {
            return new PythonPyPiLibrary()
                .setPackage(runContext.render(_package).as(String.class).orElse(null))
                .setRepo(runContext.render(repo).as(String.class).orElse(null));
        }
    }
}
