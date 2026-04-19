# Kestra Databricks Plugin

## What

- Provides plugin components under `io.kestra.plugin.databricks`.
- Includes classes such as `DeleteCluster`, `CreateCluster`, `CreateJob`, `SubmitRun`.

## Why

- What user problem does this solve? Teams need to databricks plugin for Kestra from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Databricks steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Databricks.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `databricks`

### Key Plugin Classes

- `io.kestra.plugin.databricks.cli.DatabricksCLI`
- `io.kestra.plugin.databricks.cli.DatabricksSQLCLI`
- `io.kestra.plugin.databricks.cluster.CreateCluster`
- `io.kestra.plugin.databricks.cluster.DeleteCluster`
- `io.kestra.plugin.databricks.dbfs.Download`
- `io.kestra.plugin.databricks.dbfs.Upload`
- `io.kestra.plugin.databricks.job.CreateJob`
- `io.kestra.plugin.databricks.job.SubmitRun`
- `io.kestra.plugin.databricks.sql.Query`

### Project Structure

```
plugin-databricks/
├── src/main/java/io/kestra/plugin/databricks/utils/
├── src/test/java/io/kestra/plugin/databricks/utils/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
