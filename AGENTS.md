# Kestra Databricks Plugin

## What

- Provides plugin components under `io.kestra.plugin.databricks`.
- Includes classes such as `DeleteCluster`, `CreateCluster`, `CreateJob`, `SubmitRun`.

## Why

- This plugin integrates Kestra with Databricks CLI.
- It provides tasks that execute Databricks CLI and SQL CLI commands through containerized tools.

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
