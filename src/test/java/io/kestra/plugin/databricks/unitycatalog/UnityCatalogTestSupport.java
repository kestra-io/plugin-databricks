package io.kestra.plugin.databricks.unitycatalog;

import com.databricks.sdk.WorkspaceClient;

import io.kestra.plugin.databricks.AbstractTask;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public final class UnityCatalogTestSupport {
    private UnityCatalogTestSupport() {
    }

    /**
     * Returns a spy of the task whose Databricks client is the given mock, so tasks can be run without a workspace.
     */
    public static <T extends AbstractTask> T withClient(T task, WorkspaceClient client) throws Exception {
        var spied = spy(task);
        doReturn(client).when(spied).workspaceClient(any());
        return spied;
    }
}
