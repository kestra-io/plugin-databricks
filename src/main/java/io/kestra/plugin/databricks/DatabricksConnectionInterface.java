package io.kestra.plugin.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ConfigLoader;
import com.databricks.sdk.core.DatabricksConfig;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

/**
 * Common Databricks connection properties, shared by tasks ({@link AbstractTask}) and triggers.
 */
public interface DatabricksConnectionInterface {
    Property<String> getHost();

    Property<String> getAccountId();

    Property<String> getConfigFile();

    AbstractTask.AuthenticationConfig getAuthentication();

    default WorkspaceClient workspaceClient(RunContext runContext) throws IllegalVariableEvaluationException {
        DatabricksConfig cfg = new DatabricksConfig()
            .setHost(runContext.render(getHost()).as(String.class).orElse(null))
            .setAccountId(runContext.render(getAccountId()).as(String.class).orElse(null))
            .setConfigFile(runContext.render(getConfigFile()).as(String.class).orElse(null));

        var authentication = getAuthentication();
        if (authentication != null) {
            cfg.setAuthType(runContext.render(authentication.getAuthType()).as(String.class).orElse(null))
                .setToken(runContext.render(authentication.getToken()).as(String.class).orElse(null))
                .setUsername(runContext.render(authentication.getUsername()).as(String.class).orElse(null))
                .setPassword(runContext.render(authentication.getPassword()).as(String.class).orElse(null))
                .setClientId(runContext.render(authentication.getClientId()).as(String.class).orElse(null))
                .setClientSecret(runContext.render(authentication.getClientSecret()).as(String.class).orElse(null))
                .setGoogleCredentials(runContext.render(authentication.getGoogleCredentials()).as(String.class).orElse(null))
                .setGoogleServiceAccount(runContext.render(authentication.getGoogleServiceAccount()).as(String.class).orElse(null))
                .setAzureClientId(runContext.render(authentication.getAzureClientId()).as(String.class).orElse(null))
                .setAzureClientSecret(runContext.render(authentication.getAzureClientSecret()).as(String.class).orElse(null))
                .setAzureTenantId(runContext.render(authentication.getAzureTenantId()).as(String.class).orElse(null));
        }

        // will use env var for each config that is not set
        ConfigLoader.resolve(cfg);

        return new WorkspaceClient(cfg);
    }
}
