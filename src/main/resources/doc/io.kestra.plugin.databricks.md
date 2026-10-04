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

## Lakebase

Lakebase is Databricks' managed Postgres (OLTP). It uses the standard Postgres wire protocol but authenticates with a short-lived OAuth token rather than a static password. Tokens expire after about 60 minutes, so `plugin-jdbc-postgres` with a pasted token will silently fail after expiry.

`lakebase.Query`, `lakebase.Batch`, and `lakebase.Trigger` mint a fresh credential on every connection via the Databricks SDK (`WorkspaceClient.postgres().generateDatabaseCredential`) using OAuth M2M. Set `workspaceHost`, `clientId`, `clientSecret`, `endpoint` (format `projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>`), and `database`. The service principal client ID is the Postgres username; the minted token is the password. SSL is enabled by default (`sslMode: REQUIRE`).

Optionally set `host` to the Lakebase Postgres hostname. When `host` is omitted, it is resolved from the endpoint (`status.hosts.host`). The service principal needs Workspace access and a matching Postgres OAuth role.

Do not reuse `plugin-jdbc-postgres` against Lakebase with a static token in `password` — mint a credential per execution instead.
