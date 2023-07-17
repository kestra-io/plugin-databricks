package io.kestra.plugin.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ConfigLoader;
import com.databricks.sdk.core.DatabricksConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * For more info of using the Databricks SDK, see <a href="https://github.com/databricks/databricks-sdk-java">Databricks SQK</a>
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    @PluginProperty(dynamic = true)
    @Schema(title = "Databricks host")
    private String host;

    @PluginProperty(dynamic = true)
    @Schema(title = "Databricks account identifier")
    private String accountId;

    @PluginProperty(dynamic = true)
    @Schema(title = "Databricks configuration file, use this if you don't want to configure each Databricks account properties one by one")
    private String configFile;

    @PluginProperty
    @Schema(
        title = "Databricks authentication configuration",
        description = """
            This property allows to configure the authentication to Databricks, different properties should be set depending on the type of authentication and the cloud provider.
            All configuration options can also be set using the standard Databricks environment variables.
            Check the [Databricks authentication guide](https://docs.databricks.com/dev-tools/auth.html) for more information.
            """
    )
    private AuthenticationConfig authentication;


    protected WorkspaceClient workspaceClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DatabricksConfig cfg = new DatabricksConfig()
            .setHost(runContext.render(host))
            .setAccountId(runContext.render(accountId))
            .setConfigFile(runContext.render(configFile));

        if (authentication != null) {
            cfg.setAuthType(authentication.authType)
                .setToken(runContext.render(authentication.token))
                .setUsername(runContext.render(authentication.username))
                .setPassword(runContext.render(authentication.password))
                .setClientId(runContext.render(authentication.clientId))
                .setClientSecret(runContext.render(authentication.clientSecret))
                .setGoogleCredentials(runContext.render(authentication.googleCredentials))
                .setGoogleServiceAccount(runContext.render(authentication.googleServiceAccount))
                .setAzureClientId(runContext.render(authentication.azureClientId))
                .setAzureClientSecret(runContext.render(authentication.azureClientSecret))
                .setAzureTenantId(runContext.render(authentication.azureTenantId));
        }


        // will use env var for each config that is not set
        ConfigLoader.resolve(cfg);

        return new WorkspaceClient(cfg);
    }

    @Builder
    @Getter
    public static class AuthenticationConfig {
        @PluginProperty
        private String authType;

        @PluginProperty(dynamic = true)
        private String token;

        @PluginProperty(dynamic = true)
        private String clientId;

        @PluginProperty(dynamic = true)
        private String clientSecret;

        @PluginProperty(dynamic = true)
        private String username;

        @PluginProperty(dynamic = true)
        private String password;

        @PluginProperty(dynamic = true)
        private String googleCredentials;

        @PluginProperty(dynamic = true)
        private String googleServiceAccount;

        @PluginProperty(dynamic = true)
        private String azureClientId;

        @PluginProperty(dynamic = true)
        private String azureClientSecret;

        @PluginProperty(dynamic = true)
        private String azureTenantId;
    }
}
