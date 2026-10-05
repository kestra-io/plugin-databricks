package io.kestra.plugin.databricks;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * For more info on using the Databricks SDK, see <a href="https://github.com/databricks/databricks-sdk-java">Databricks SDK</a>
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractTask extends Task implements DatabricksConnectionInterface {
    @Schema(title = "Databricks host")
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(title = "Databricks account identifier")
    @PluginProperty(group = "advanced")
    private Property<String> accountId;

    @Schema(title = "Databricks configuration file, use this if you don't want to configure each Databricks account properties one by one")
    @PluginProperty(group = "advanced")
    private Property<String> configFile;

    @PluginProperty(dynamic = true, group = "connection")
    @Schema(
        title = "Databricks authentication configuration",
        description = """
            This property allows to configure the authentication to Databricks, different properties should be set depending on the type of authentication and the cloud provider.
            All configuration options can also be set using the standard Databricks environment variables.
            Check the [Databricks authentication guide](https://docs.databricks.com/dev-tools/auth.html) for more information.
            """
    )
    private AuthenticationConfig authentication;

    @Builder
    @Getter
    public static class AuthenticationConfig {
        @Schema(title = "Authentication type")
        private Property<String> authType;

        @Schema(title = "Databricks personal access token")
        @PluginProperty(secret = true)
        private Property<String> token;

        @Schema(title = "Client ID")
        private Property<String> clientId;

        @Schema(title = "Client secret")
        @PluginProperty(secret = true)
        private Property<String> clientSecret;

        @Schema(title = "Username")
        private Property<String> username;

        @Schema(title = "Password")
        @PluginProperty(secret = true)
        private Property<String> password;

        @Schema(title = "Google credentials JSON")
        @PluginProperty(secret = true)
        private Property<String> googleCredentials;

        @Schema(title = "Google service account email")
        private Property<String> googleServiceAccount;

        @Schema(title = "Azure client ID")
        private Property<String> azureClientId;

        @Schema(title = "Azure client secret")
        @PluginProperty(secret = true)
        private Property<String> azureClientSecret;

        @Schema(title = "Azure tenant ID")
        private Property<String> azureTenantId;
    }
}
