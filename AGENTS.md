# Kestra Databricks Plugin

## What

Databricks plugin for Kestra Exposes 9 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Databricks, allowing orchestration of Databricks-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
