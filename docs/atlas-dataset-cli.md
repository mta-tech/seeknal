# Atlas data catalog from the CLI (`seeknal dataset`)

When Atlas is configured, the `seeknal dataset` command group gives the CLI the
same capabilities as the Atlas web portal — discover datasets, inspect their
schema and lineage, preview governed rows, request access, and wire a governed
dataset straight into a pipeline. Every read flows the logged-in user's identity
to Atlas, so you see exactly what you are permitted to see.

## Configure (one command)

Point the CLI at your Atlas host once — it derives the seeknal-api, portal, and
Keycloak URLs and persists them to `~/.config/seeknal/atlas.json`, so no per-shell
env vars are needed:

```bash
seeknal auth config set --host atlas-dev-server   # api :8000 · portal :4200 · keycloak :8080/realms/atlas
seeknal auth login                                # sign in (Keycloak PKCE)
seeknal dataset list                              # ready — no env vars
```

Or do both in one step, and inspect/override later:

```bash
seeknal auth login --host atlas-dev-server   # configure + sign in together
seeknal auth config show                     # effective values + where each comes from (env/config/default)
```

Non-standard deployment? Override any piece: `--api-url`, `--portal-url`,
`--keycloak-issuer`, `--realm`, `--client-id`. **Environment variables still win**
over the config file (`ATLAS_API_URL`, `ATLAS_PORTAL_URL`, `KEYCLOAK_ISSUER`,
`SEEKNAL_OIDC_CLIENT_ID`), so CI/automation can override without editing the file.

With none of these set the whole group is inactive (commands exit with a hint) and
`seeknal run` is ungoverned — full backward compatibility.

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
