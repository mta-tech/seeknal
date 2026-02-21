---
summary: Manage Apache Iceberg tables and Atlas Data Platform integration
read_when: You need to work with Iceberg tables or data governance features
related:
  - iceberg
  - validate
---

# seeknal atlas

The Atlas command group provides integration with the Atlas Data Platform for
data governance, lineage tracking, and API services. This is an optional feature
that requires the `atlas-data-platform` package.

## Synopsis

```bash
seeknal atlas [COMMAND] [OPTIONS]
```

## Description

Atlas commands enable advanced data platform features including:

- **API Server**: Start a REST API for feature serving
- **Governance**: View governance statistics, policies, and violations
- **Lineage**: Track and publish data lineage information

**Note:** Atlas integration requires installing the optional dependency:
```bash
pip install seeknal[atlas]
```

## Commands

| Command | Description |
|---------|-------------|
| `atlas info` | Show Atlas integration information |
| `atlas api start` | Start the Atlas API server |
| `atlas api status` | Check API server status |
| `atlas governance stats` | Display governance statistics |
| `atlas governance policies` | List governance policies |
| `atlas governance violations` | List policy violations |
| `atlas lineage show <name>` | Show lineage for a resource |
| `atlas lineage publish <pipeline>` | Publish lineage to DataHub |

## Examples

### Check Atlas status

```bash
seeknal atlas info
```

### Start the API server

```bash
seeknal atlas api start --port 8000
```

### View governance statistics

```bash
seeknal atlas governance stats
```

### Show lineage for a feature group

```bash
seeknal atlas lineage show user_features
```

## See Also

- [seeknal iceberg](../materialization_cli.md) - Iceberg materialization commands
- [seeknal validate](validate.md) - Validate configurations
