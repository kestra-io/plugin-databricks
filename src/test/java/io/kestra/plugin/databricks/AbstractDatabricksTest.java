package io.kestra.plugin.databricks;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.cluster.CreateCluster;
import jakarta.inject.Inject;
import org.assertj.core.util.Strings;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.DisabledIf;

import java.util.Map;

@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: host, token"
)
public class AbstractDatabricksTest {
    protected static final String HOST = System.getenv("DATABRICKS_HOST");
    protected static final String TOKEN = System.getenv("DATABRICKS_TOKEN");
    protected static final String CLUSTER_ID = System.getenv("DATABRICKS_CLUSTER_ID");

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN) || Strings.isNullOrEmpty(System.getenv(CLUSTER_ID));
    }
}
