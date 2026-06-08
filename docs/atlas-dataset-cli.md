# Atlas data catalog from the CLI (`seeknal dataset`)

When Atlas is configured, the `seeknal dataset` command group gives the CLI the
same capabilities as the Atlas web portal — discover datasets, inspect their
schema and lineage, preview governed rows, request access, and wire a governed
dataset straight into a pipeline. Every read flows the logged-in user's identity
to Atlas, so you see exactly what you are permitted to see.

## Configure

```bash
export ATLAS_API_URL=http://atlas-dev-server:8000        # seeknal-api (gate + sample)
export ATLAS_PORTAL_URL=http://atlas-dev-server:4200     # portal catalog (list/lineage parity with the web)
export KEYCLOAK_ISSUER=http://atlas-dev-server:8080/realms/atlas
seeknal auth login                                       # or a non-interactive token mint
```

With `ATLAS_API_URL` unset the whole group is inactive (commands exit with a hint),
and `seeknal run` is ungoverned — full backward compatibility.

## Discover → inspect

```bash
seeknal dataset list                       # the same catalog the web shows (Lakekeeper tables + cubes)
seeknal dataset list -q sales --type table
seeknal dataset show retail_demo.sales     # metadata + columns/schema + your live ALLOW/DENY
seeknal dataset lineage retail_demo.sales  # upstream/downstream (DataHub-backed)
seeknal dataset query retail_demo.sales --limit 5   # governed, access-checked + masked preview
```

`show` and `query` print your live access decision; a denied `query` points you at
`request-access`.

## Use a governed dataset in a pipeline

```bash
seeknal dataset use retail_demo.sales --write    # scaffolds seeknal/sources/sales.yml
```

This writes a governed iceberg source:

```yaml
kind: source
name: sales
source: iceberg
table: atlas.retail_demo.sales
# catalog_uri + warehouse resolve from profiles.yml (materialization.catalog) or source_defaults.iceberg.
# `seeknal run` access-checks + masks this table via the Atlas gate.
```

Reference it in a transform with `ref('source.sales')` and run:

```bash
seeknal run
```

`seeknal run` enforces access on every source node before it runs (`_enforce_pre_run`):
a dataset you have no grant for aborts the run with the Atlas reason — governance is
not optional and the rules live in Atlas, not in editable local files.

## Request access · annotate

```bash
seeknal dataset request-access retail_demo.sales --reason "need it for the churn model"
seeknal dataset annotate retail_demo.sales --tag pii --owner data-team
```

`request-access` files an Atlas access request (auto-refreshing an expired session
once before giving up); annotations are written back to the unified catalog.
