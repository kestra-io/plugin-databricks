# How to use the Databricks plugin

Run jobs, manage clusters, execute SQL, and move files on Databricks from Kestra flows.

## Authentication

Set `host` to your Databricks workspace URL and configure `authentication` with the appropriate credential type. For personal access token auth, set `authentication.token`. For OAuth M2M, set `authentication.clientId` and `authentication.clientSecret`. For Azure-hosted workspaces, use `authentication.azureClientId`, `authentication.azureClientSecret`, and `authentication.azureTenantId`. Alternatively, point `configFile` to a Databricks CLI configuration file. Store all secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`job.CreateJob` creates a Databricks job — set `jobName` and `jobTasks` (a list of task settings). `job.SubmitRun` submits a one-off run without creating a persistent job — set `runName` and `runTasks`. Both accept `waitForCompletion` to block until the run finishes. Each task entry supports multiple execution types: `NotebookTaskSetting` (`notebookPath`), `SparkPythonTaskSetting` (`pythonFile`), `SparkJarTaskSetting` (`jarUri`, `mainClassName`), `SqlTaskSetting` (`warehouseId`, `queryId`), `DbtTaskSetting` (`commands`, `warehouseId`), and `PipelineTaskSetting` (`pipelineId`). Attach libraries to any task via a `libraries` list (JAR, PyPI, Maven, wheel, or egg).

`cluster.CreateCluster` provisions a cluster — set `clusterName`, `sparkVersion`, and `nodeTypeId`. Use `numWorkers` for a fixed size or `minWorkers`/`maxWorkers` for autoscaling. Set `autoTerminationMinutes` to terminate idle clusters automatically. `cluster.DeleteCluster` removes a cluster by `clusterId`.

`sql.Query` runs a SQL query against a Databricks SQL warehouse — set `host`, `httpPath`, `accessToken`, and `sql`. Optionally scope to a `catalog` and `schema`. Results are streamed to internal storage.

`dbfs.Upload` uploads a file from Kestra internal storage to DBFS — set `from` (a `kestra://` URI) and `to` (the DBFS destination path). `dbfs.Download` retrieves a file from DBFS by `from` path.
