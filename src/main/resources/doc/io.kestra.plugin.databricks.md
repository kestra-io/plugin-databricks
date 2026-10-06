# How to use the Databricks plugin

Run jobs, manage clusters, execute SQL, ask Genie, govern Unity Catalog, and move files on Databricks from Kestra flows.

## Authentication

Set `host` to your Databricks workspace URL and configure `authentication` with the appropriate credential type. For personal access token auth, set `authentication.token`. For OAuth M2M, set `authentication.clientId` and `authentication.clientSecret`. For Azure-hosted workspaces, use `authentication.azureClientId`, `authentication.azureClientSecret`, and `authentication.azureTenantId`. Alternatively, point `configFile` to a Databricks CLI configuration file. Store all secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

## Tasks

`job.CreateJob` creates a Databricks job — set `jobName` and `jobTasks` (a list of task settings). `job.SubmitRun` submits a one-off run without creating a persistent job — set `runName` and `runTasks`. Both accept `waitForCompletion` to block until the run finishes. Each task entry supports multiple execution types: `NotebookTaskSetting` (`notebookPath`), `SparkPythonTaskSetting` (`pythonFile`), `SparkJarTaskSetting` (`jarUri`, `mainClassName`), `PythonWheelTaskSetting`, `PipelineTaskSetting` (`pipelineId`), and `RunJobTaskSetting` (`jobId`); `SqlTaskSetting` (`warehouseId`, `queryId`) and `DbtTaskSetting` (`commands`, `warehouseId`) are available on `CreateJob` only. Attach libraries to any task via a `libraries` list (JAR, PyPI, Maven, CRAN, wheel, or egg).

`cluster.CreateCluster` provisions a cluster — set `clusterName` and `sparkVersion` (required), and optionally `nodeTypeId`. Use `numWorkers` for a fixed size or `minWorkers`/`maxWorkers` for autoscaling. Set `autoTerminationMinutes` to terminate idle clusters automatically. `cluster.DeleteCluster` removes a cluster by `clusterId`.

`job.SubmitRun` always attaches a Databricks idempotency token derived from the Kestra task run id, so a worker-loss resubmit adopts the already in-flight or already completed run instead of launching a duplicate one; a plain task retry after a failure still creates a genuinely new run. Set the optional `idempotencyToken` property only if you need to key deduplication on something other than the task run itself — two executions sharing the same override value will cause the second one to adopt the first one's run. This does not dedup runs across separate Kestra executions or runs started outside Kestra.

`sql.Query` runs a SQL query against a Databricks SQL warehouse — set `host`, `httpPath`, `accessToken`, and `sql`. Optionally scope to a `catalog` and `schema`. Results are streamed to internal storage.

`dbfs.Upload` uploads a file from Kestra internal storage to DBFS — set `from` (a `kestra://` URI) and `to` (the DBFS destination path). `dbfs.Download` retrieves a file from DBFS by `from` path. Note: Databricks considers DBFS legacy per [official guidance](https://docs.databricks.com/aws/en/dbfs/unity-catalog); for new flows, use Unity Catalog Volumes tasks (`unitycatalog.volume.Upload` and `unitycatalog.volume.Download`) instead.

`cli.DatabricksCLI` runs Databricks CLI commands in a container — set `commands` (the `host` and authentication are passed to the CLI via environment variables). `cli.DatabricksSQLCLI` runs SQL statements through the Databricks SQL CLI — set `commands` along with the connection properties, and use `outputFiles` to persist the CLI output.

`genie.AskQuestion` asks a question of an existing Genie space — set `spaceId` and `question`. It blocks until Genie answers; `timeout` is an ISO-8601 duration (default 20 minutes) and must be greater than zero. `genie.Continue` sends a follow-up — set `conversationId` from the previous task. The space has to exist first. A text answer is returned on `text`. Generated SQL is returned on `query` with rows on `result`; a text-only answer leaves those unset. At most `maxRows` rows are returned (default 1000); a larger result fails and names `maxRows` and the row count.

## Unity Catalog

The `unitycatalog` tasks manage the Unity Catalog governance layer with the same `host` and `authentication` properties as every other task. Objects follow the three-level namespace `catalog.schema.table|volume`: creation and listing tasks take the parent parts (`catalogName`, `schemaName`) and a `name`, while `Get`, `Update` and `Delete` address a single object by `fullName` (`catalog.schema`, `catalog.schema.volume`, ...). Catalogs are addressed by `name`.

`unitycatalog.catalog` and `unitycatalog.schema` provide `Create`, `List`, `Get`, `Update` and `Delete`; set `force` on `Delete` to remove a non-empty object. `unitycatalog.table` provides `List`, `Get` and `Delete` only — tables are created with SQL, for example through `sql.Query`. `unitycatalog.table.Trigger` polls a schema every `interval` (default `PT5M`) and starts one execution for the tables discovered since the previous poll, exposed as `trigger.tables`, `trigger.fullNames` and `trigger.size`; known tables are persisted in the namespace KV Store, so with the default `on: CREATE` the first poll reports every existing table.

`unitycatalog.volume` provides `Create` (`MANAGED` by default; `EXTERNAL` requires `storageLocation`), `List`, `Get`, `Update` and `Delete` for volume metadata, plus `Upload` and `Download` for file content through the Databricks Files API, the replacement for the legacy DBFS tasks. Both take a `volumePath` in the form `/Volumes/<catalog>/<schema>/<volume>/<path>` and the volume must already exist; `Upload` reads `from` (a `kestra://` URI) and fails on an existing file unless `overwrite` is `true`, `Download` returns a storage `uri`.

`unitycatalog.grant.Get` returns the privileges granted directly on an object, set `securableType` (for example `SCHEMA`) and `fullName`, and optionally `principal`. `unitycatalog.grant.Update` applies a list of `changes`, each with a `principal` and the privileges to `add` or `remove`.
