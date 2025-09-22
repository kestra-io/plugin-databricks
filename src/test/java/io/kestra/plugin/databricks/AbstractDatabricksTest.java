package io.kestra.plugin.databricks;

import org.assertj.core.util.Strings;
import org.junit.jupiter.api.condition.DisabledIf;

@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: host, token"
)
public class AbstractDatabricksTest {
    protected static final String HOST = System.getenv("DATABRICKS_HOST");
    protected static final String TOKEN = System.getenv("DATABRICKS_TOKEN");

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN);
    }
}
