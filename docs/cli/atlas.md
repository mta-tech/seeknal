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
- **Feature Services**: Publish explicit immutable serving contracts
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
| `atlas feature-service publish <yaml>` | Publish an immutable Feature Service contract |

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

### Publish a Feature Service

Configure Atlas and sign in once:

```bash
seeknal auth config set --host atlas-dev-server
seeknal auth login
```

The bare host derives the standard endpoints, including the Seeknal API at
`http://atlas-dev-server:8000`. For a non-standard deployment, pass the API
explicitly with `--api-url`.

Create `customer-analytics.yml`:

```yaml
schemaVersion: 1
serviceId: customer-analytics
version: "1"
variant: default
owner: ml-platform
description: Customer features for analytics models
tags: [customer, ml]
entityKeys:
  - semanticName: customer_id
    physicalName: customer_id
    dataType: string
    ordinal: 0
selections:
  - view:
      viewId: customer_profile
      revision: v1
      schemaRevision: v1
      sourceLocator: seeknal:feature-group:customer_profile:v1
      fields:
        - name: age
          dataType: int64
        - name: lifetime_value
          dataType: float64
      entityKeys:
        - semanticName: customer_id
          physicalName: customer_id
          dataType: string
          ordinal: 0
    features: [age, lifetime_value]
    ordinal: 0
```

Publish it:

```bash
seeknal atlas feature-service publish customer-analytics.yml
```

The command is idempotent: an identical replay returns the existing immutable
version, while schema or metadata drift for the same identity is rejected.
`seeknal apply` continues to register Feature Groups as catalog assets; it does
not infer a production Feature Service from each group. Publishing metadata also
does not grant serving access—an operator must separately provision the
Feature Service policy binding and OpenFGA tuples.

## See Also

- [seeknal iceberg](../materialization_cli.md) - Iceberg materialization commands
- [seeknal validate](validate.md) - Validate configurations
