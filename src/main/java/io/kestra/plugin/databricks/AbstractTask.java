package io.kestra.plugin.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ConfigLoader;
import com.databricks.sdk.core.DatabricksConfig;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * See https://github.com/databricks/databricks-sdk-java
 * See auth https://docs.databricks.com/dev-tools/auth.html
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task {
    //TODO there is a lot of props, maybe organize them to avoid too much props that will clutter the doc.
    @PluginProperty(dynamic = true)
    private String host;

    @PluginProperty(dynamic = true)
    private String accountId;

    @PluginProperty
    private String authType;

    @PluginProperty(dynamic = true)
    private String configFile;

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

    protected WorkspaceClient workspaceClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DatabricksConfig cfg = new DatabricksConfig()
            .setHost(runContext.render(host))
            .setAccountId(runContext.render(accountId))
            .setAuthType(authType)
            .setToken(runContext.render(token))
            .setUsername(runContext.render(username))
            .setPassword(runContext.render(password))
            .setClientId(runContext.render(clientId))
            .setClientSecret(runContext.render(clientSecret))
            .setConfigFile(runContext.render(configFile))
            .setGoogleCredentials(runContext.render(googleCredentials))
            .setGoogleServiceAccount(runContext.render(googleServiceAccount))
            .setAzureClientId(runContext.render(azureClientId))
            .setAzureClientSecret(runContext.render(azureClientSecret))
            .setAzureTenantId(runContext.render(azureTenantId));

        // will use env var for each config that is not set
        ConfigLoader.resolve(cfg);

        return new WorkspaceClient(cfg);
    }
}
