# How to use the Databricks plugin

Run jobs, manage clusters, execute SQL, and move files on Databricks from Kestra flows.

## Authentication

Set `host` to your Databricks workspace URL and configure `authentication` with the appropriate credential type. For personal access token auth, set `authentication.token`. For OAuth M2M, set `authentication.clientId` and `authentication.clientSecret`. For Azure-hosted workspaces, use `authentication.azureClientId`, `authentication.azureClientSecret`, and `authentication.azureTenantId`. Alternatively, point `configFile` to a Databricks CLI configuration file. Store all secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

`job.CreateJob` creates a Databricks job — set `jobName` and `jobTasks` (a list of task settings). `job.SubmitRun` submits a one-off run without creating a persistent job — set `runName` and `runTasks`. Both accept `waitForCompletion` to block until the run finishes. Each task entry supports multiple execution types: `NotebookTaskSetting` (`notebookPath`), `SparkPythonTaskSetting` (`pythonFile`), `SparkJarTaskSetting` (`jarUri`, `mainClassName`), `SqlTaskSetting` (`warehouseId`, `queryId`), `DbtTaskSetting` (`commands`, `warehouseId`), and `PipelineTaskSetting` (`pipelineId`). Attach libraries to any task via a `libraries` list (JAR, PyPI, Maven, wheel, or egg).

`job.SubmitRun` always attaches a Databricks idempotency token derived from the Kestra task run id, so a worker-loss resubmit adopts the already in-flight or already completed run instead of launching a duplicate one; a plain task retry after a failure still creates a genuinely new run. Set the optional `idempotencyToken` property only if you need to key deduplication on something other than the task run itself — two executions sharing the same override value will cause the second one to adopt the first one's run. This does not dedup runs across separate Kestra executions or runs started outside Kestra.

`cluster.CreateCluster` provisions a cluster — set `clusterName`, `sparkVersion`, and `nodeTypeId`. Use `numWorkers` for a fixed size or `minWorkers`/`maxWorkers` for autoscaling. Set `autoTerminationMinutes` to terminate idle clusters automatically. `cluster.DeleteCluster` removes a cluster by `clusterId`.

`sql.Query` runs a SQL query against a Databricks SQL warehouse — set `host`, `httpPath`, `accessToken`, and `sql`. Optionally scope to a `catalog` and `schema`. Results are streamed to internal storage.

`dbfs.Upload` uploads a file from Kestra internal storage to DBFS — set `from` (a `kestra://` URI) and `to` (the DBFS destination path). `dbfs.Download` retrieves a file from DBFS by `from` path.
