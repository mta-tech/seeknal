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
| `atlas feature-service plan <selector>` | Validate applied serving evidence without publishing |
| `atlas feature-service compile <selector>` | Emit the canonical Atlas contract from applied state |
| `atlas feature-service publish <selector>` | Publish an immutable Feature Service contract |

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

Create the serving Feature Group:

```yaml
kind: feature_group
name: customer_profile
owner: ml-platform
entity:
  name: customer
  join_keys: [customer_id]
features:
  customer_id:
    dtype: string
  age:
    dtype: integer
  lifetime_value:
    dtype: float
  observed_at:
    dtype: timestamp
inputs:
  - ref: transform.customer_profile
materializations:
  - type: atlas_online
    connection: atlas_feature_store
    table: customer_profile
    mode: replace
    event_time_column: observed_at
    ttl_seconds: 86400
```

Then declare the contract-only Feature Service:

```yaml
kind: feature_service
name: customer-analytics
version: "1"
variant: default
owner: ml-platform
description: Customer features for analytics models
tags: [customer, ml]
views:
  - ref: feature_group.customer_profile
    features: [age, lifetime_value]
```

Run the Feature Group, inspect the exact applied contract, then publish it:

```bash
seeknal parse
seeknal run --profile profiles.yml --full
seeknal atlas feature-service plan feature_service.customer-analytics
seeknal atlas feature-service compile feature_service.customer-analytics
seeknal atlas feature-service publish feature_service.customer-analytics
```

Compilation reads `target/manifest.json` and `target/run_state.json`. It fails
unless every selected Feature Group has a successful `atlas_online`
materialization whose revision and schema hash match the applied fingerprint.
This prevents publishing a YAML intention that is not actually serving.

The publish command is idempotent: an identical replay returns the existing
immutable version, while contract drift for the same identity is rejected.
Raw contract snapshots, including legacy `schemaVersion: 1` YAML, cannot be
published. Define the Feature Service in the Seeknal project using YAML or the
Python builder, run the project to materialize and record applied state, then
publish its `feature_service.<name>` selector. This keeps every published
contract anchored to applied Feature Group and Feature Service state.

Seeknal owns Feature Group execution, the atomic staging-to-live table swap, and
publication evidence. Atlas/Data Catalog owns immutable contract registration,
read-only serving, governance, and UI. Publishing metadata does not grant
serving access—an operator must separately provision the Feature Service policy
binding and OpenFGA tuples.

See the runnable YAML and Python projects under
`examples/feature-serving-e2e/`.

## See Also

- [seeknal iceberg](../materialization_cli.md) - Iceberg materialization commands
- [seeknal validate](validate.md) - Validate configurations
