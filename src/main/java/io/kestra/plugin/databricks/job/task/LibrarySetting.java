package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.compute.Library;
import com.databricks.sdk.service.compute.MavenLibrary;
import com.databricks.sdk.service.compute.PythonPyPiLibrary;
import com.databricks.sdk.service.compute.RCranLibrary;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class LibrarySetting {
    @PluginProperty
    private CranSetting cran;

    @PluginProperty(dynamic = true)
    private String egg;

    @PluginProperty(dynamic = true)
    private String jar;

    @PluginProperty
    private MavenSetting maven;

    @PluginProperty
    private PypiSetting pypi;

    @PluginProperty(dynamic = true)
    private String whl;

    public Library toLibrary(RunContext runContext) throws IllegalVariableEvaluationException {
        return new Library()
            .setCran(cran != null ? cran.toCran(runContext) : null)
            .setEgg(runContext.render(egg))
            .setJar(runContext.render(jar))
            .setMaven(maven != null ? maven.toMaven(runContext) : null)
            .setPypi(pypi != null ? pypi.toPypi(runContext) : null)
            .setWhl(runContext.render(whl));
    }

    @Builder
    @Getter
    public static class CranSetting {
        @PluginProperty(dynamic = true)
        private String _package;

        @PluginProperty(dynamic = true)
        private String repo;

        public RCranLibrary toCran(RunContext runContext) throws IllegalVariableEvaluationException {
            return new RCranLibrary()
                .setPackage(runContext.render(_package))
                .setRepo(runContext.render(repo));
        }
    }

    @Builder
    @Getter
    public static class MavenSetting {
        @PluginProperty(dynamic = true)
        private String coordinates;

        @PluginProperty(dynamic = true)
        private String repo;

        @PluginProperty(dynamic = true)
        private List<String> exclusions;

        public MavenLibrary toMaven(RunContext runContext) throws IllegalVariableEvaluationException {
            return new MavenLibrary()
                .setCoordinates(coordinates)
                .setExclusions(exclusions != null ? runContext.render(exclusions) : null)
                .setRepo(runContext.render(repo));
        }
    }

    @Builder
    @Getter
    public static class PypiSetting {
        @PluginProperty(dynamic = true)
        private String _package;

        @PluginProperty(dynamic = true)
        private String repo;

        public PythonPyPiLibrary toPypi(RunContext runContext) throws IllegalVariableEvaluationException {
            return new PythonPyPiLibrary()
                .setPackage(runContext.render(_package))
                .setRepo(runContext.render(repo));
        }
    }
}
