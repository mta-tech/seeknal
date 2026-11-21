# Seeknal Serving: Postgres and Lakebase sources, storage-class PII placement, filter guardrails, select API, safeData hook

Design, 2026-11-19, revision 3.

**Base:** `spike/iceberg-direct` @ `5644d56`, plus the result of P0 run `run-8b333b2ff92439d5cd2f` (A–F) once it is imported and verified.

**Citations**

| Prefix | Points to |
|---|---|
| (none) | `file:line` in Serving `src/seeknal/serving/` at `5644d56` |
| `lb:` | `/Users/fitrakacamarga/project/mta/atlas-lakebase` at `849418b` |
| `neon:` | the pinned Neon contracts in `lb:contracts/neon/` (Neon commit `fa50421`) |
| `safe:` | the safe agent's consult, `research/consult-safe/a-atlas-main-cbn-pii.md` |
| `lbc:` | the atlas-lakebase orchestrator's consult, `research/consult-lakebase/a-atlas-main-serving-lakebase.md`. **[live 11-10]** marks its measurements on the GCP pilot. |

**[GK]** marks general knowledge that was not verified here. **Not built** means nothing in the code does it today.

**Revision 3** splits sources into two kinds:
- `postgres`: plain Postgres on local or attached storage;
- `lakebase`: Atlas Lakebase, which is Postgres on Neon storage persisted to object storage.

Rev 3 adds:
- a shared wire layer, plus the Lakebase-only parts;
- `storage_class` on every engine;
- the PII storage rule;
- a migration path for legacy `kind: lakebase`;
- `postgres_*` naming for the shared layer.

The safeData hook (§9) and the sync-state alignment from rev 2 are kept.

**Rev 3 final** takes in the atlas-lakebase consult (`lbc:`). Its facts replace these earlier Neon assumptions:
- **Startup options:** passed through the L4 router, so the PgBouncer **[GK]** concern does not apply to Lakebase.
- **Read-only endpoints:** they serve frozen snapshots, not streaming replicas.
- **LSN freshness:** dropped in favour of data-level markers.
- **Resume time:** 2.3–5.2 s live, not 2.2–3.6 s.
- **Wake:** an explicit wake API is now in the MVP, not phase 2, because restarts happen even with suspend off.
- **Role guard:** API roles are `neon_superuser` with BYPASSRLS.
- **TLS:** `verify-full` is mandatory.
- **Storage:** data also lands on local disks, and the compute↔storage hop has no TLS.
- **Readiness:** an on-prem Lakebase at CBN next week is not credible.

The cold-cache behaviour after resume remains **[GK]**.

## 0. Decisions at a glance

| # | Decision |
|---|---|
| D1 | **Two source kinds over one Postgres-wire layer.** `postgres` is plain PostgreSQL on local or attached block storage (CBN's report DB, the DB Staging replica). `lakebase` is Atlas Lakebase: Postgres compute on Neon pageserver and safekeepers that persist to object storage, with projects, branches, endpoints and suspend. Both share one adapter, one pool, session hardening, accounts, TLS and pg_catalog discovery. `lakebase` adds endpoint and branch metadata, a mandatory `verify-full`, a role guard against `neon_superuser`, a single-flight wake through the control-plane start API, snapshot semantics for `read_only` endpoints, `_lakebase_sync_state` freshness on by default, and a fixed `storage_class: object_storage`. It is built for the GCP pilot and later use. CBN on-prem serves from `postgres` (`lbc:` §7). |
| D2 | **Named sources form a map: `connections.<name>: {kind: postgres\|lakebase, ...}`.** That is the existing pattern. The name is the identity that placement serves, audit and metric labels refer to, and duplicate names are rejected. |
| D3 | **Every engine has a `storage_class`: `local` or `object_storage`.** A `pii: true` table may be served only by a `local` engine, unless the policy key `pii_object_storage_engines` names that engine. This is a startup check that extends P0's E. So CBN's PII report database is a `postgres` source, never `lakebase`. |
| D4 | **Backward compatibility.** `kind: lakebase` **without** a `lakebase:` endpoint block is accepted as *legacy Lakebase*. It behaves exactly as today and warns that it is deprecated: relabel it `kind: postgres`, or add the block. Its `storage_class` is `unknown`, which the PII rule treats as object storage. No existing config breaks, because `pii: true` is new in P0 and no current config uses it. |
| D5 | Postgres-wire engines are the store for the `point` and `filter` patterns. DuckDB (accel file or `iceberg_direct`) and StarRocks take scans and aggregates. Cube keeps semantic queries. |
| D6 | Each table gets `access:` metadata: pattern, filterable, range and prefix filterable, sortable, `require_filter`, `max_rows`. It is derived from indexes, and overrides win. One guardrail module serves both the SQL path and the select API. |
| D7 | The MVP ships `POST /v1/select`, a Hasura-style JSON select over the same AST and the same ES256 grant, with `sql_sha256` over the exact body bytes plus `api: "select.v1"`. GraphQL comes later as a facade (012). |
| D8 | From Cube: named sources and per-table access metadata now; measures and rollups later. Serving does not rebuild Cube. |
| D9 | Same-source joins are allowed. Cross-source joins are rejected with F's `query_spans_engines`. Relationships (010) come later. |
| D10 | The safeData reveal hook is designed now and built in a later run (R2), gated on safeData's readiness (§9). Tables holding plaintext PII still obey D3. Tokenized tables are exempt. |

## 1. What exists today, and what Lakebase and Neon add

### Serving at 5644d56

- **Engine kinds** are `{starrocks, cube, lakebase, duckdb}` (`placement.py:12`).
  - Several `lakebase` connections can be configured, each with its own `LakebasePool` (`server/app.py:232-250`). Each has a single `dsn_env` and no accounts (`server/config.py:357-366,1095-1135`). StarRocks has accounts (`server/config.py:973-981`, `adapters.py:1584-1622`).
  - **Today's `lakebase` kind is in practice plain Postgres.** The CBN lab declares `lb: {kind: lakebase, dialect: postgres}` (`cbn-reporting-20261110/serving-runtime/lab/vm/config/placement.yaml:4`). The atlas-dev template leaves Lakebase out entirely (`atlas: deploy/atlas-dev-server/serving/serving.yaml.template:74`).
- **Session handling.**
  - Connecting is a bare `psycopg2.connect(dsn, connect_timeout)` (`server/app.py:389-401`).
  - Each query runs `BEGIN`, then `SET TRANSACTION READ ONLY` only when `row_limit` is set, then `SET LOCAL statement_timeout` (`adapters.py:1239-1243`).
  - A failed pooled connect is retried once, immediately (`adapters.py:1217-1219`, `max_attempts = 2`).
  - **Not built:** session-level read-only, `lock_timeout`, an idle-in-transaction timeout, a pinned `search_path`, `application_name`, TLS keys, and any verification that these took effect.
- **Routing.**
  - An engine is feasible only if it serves every referenced table (`router.py:331-359`). A cross-source query therefore fails (`router.py:557-559`).
  - `point_lookup` takes `=`/`IN` on the keys and sends it to `lakebase` (`router.py:419-457`). Everything else goes to `analytics` (`:462-513`) or `small_scan` (`:515-529`).
  - Feature extraction recognises only `=`/`IN` (`features.py:124-149`).
  - Writes go only to `lakebase` (`router.py:371-377,396-413`).
- **Discovery.**
  - Only Lakekeeper is read (`discovery/build.py:140-220`).
  - A Lakebase serve exists only for a synced table, with lag taken from `_lakebase_sync_state` (`build.py:184-208`, `discovery/lakebase.py:27-73`). That table is the contract `lb:integrations/atlas/src/lakebase_atlas/synced_tables.py:107-165` writes.
  - The sync-state query is unqualified (`discovery/lakebase.py:34`).
  - A failed refresh keeps the old catalog (`refresh.py:59-76`).
- **Pipeline order.**
  1. Grant (`service.py:240`).
  2. Digest (`:262`).
  3. Route (`:332`).
  4. `enforce` (`:351`): masks, row filters, allowlist, and the row cap (`enforce.py:1457-1496`). Masked columns may appear only as bare projections (`enforce.py:1247`).
  5. Render (`:368-377`).

### Atlas Lakebase and Neon facts that matter for serving

| Fact | Source | Serving consequence |
|---|---|---|
| Project → branch (a Neon timeline) → endpoint (a compute, `read_write` or `read_only`) plus roles and databases. At most one `read_write` endpoint per branch, and `read_write` is the default. IDs are `prj_`/`br_`/`ep_` + 20 lowercase base32 characters. | `lb:docs/architecture.md:36-48`; `lb:docs/api/lakebase-api.yaml:358,410` | The `lakebase:` block metadata, and `endpoint_type`. |
| **Connection.** Each endpoint has one stable port on a pure-L4 HAProxy router (40000–49999). There is no Neon proxy and no SNI. TLS terminates in the compute, plaintext is refused, and clients use `verify-full` with the site CA. Startup `options` pass through unchanged; the console enforces `default_transaction_read_only` that way. There is no pooler, and each endpoint has `max_connections = max(25, CU×100)`. | `lbc:` §1; `lb:docs/decisions/0004-routing.md:5-9`; `lb:apps/console/src/lakebase_console/sql.py:71-75`; `lb:services/control-plane/src/lakebase_cp/neon/spec.py:84` | Startup-option hardening works on `lakebase`. `sslmode` weaker than `verify-full` is rejected. The pool budget is checked against `max_connections`. |
| **Auth.** Password only (SCRAM-SHA-256), with no JWT login. Roles created through the API are members of `neon_superuser` and have BYPASSRLS. | `lbc:` §1; ADR-0012; `lb:docs/security-phi.md:20-23` | Serving must connect as a role **created in SQL**. A role guard runs at pool start (§2.2). |
| **Storage.** WAL is written to 3 safekeepers' local disks, then offloaded to the bucket (partial backup every 1 min). Page layers sit on the pageserver's local disk and are uploaded to `remote_storage`. Compute pgdata and spill files are on local disk. There is no supported way to keep a database off object storage. The compute↔pageserver/safekeeper hop uses per-tenant JWT auth but has **no TLS** in the pilot. | `lbc:` §6; `lb:deploy/templates/pageserver.toml.j2:12-20`; `lb:docs/security-phi.md:25-27,54` | `storage_class: object_storage`, fixed. Under CBN's rule, PII on Lakebase means PII on S3 and on several local disks. |
| **Compute modes.** `Primary`, `Static(Lsn)` and `Replica`. A `read_only` endpoint is rendered as `Replica`, **but it does not stream WAL**: it serves a frozen snapshot taken when it starts. Suspend plus resume refreshes it in about 4 s. The fix is not scheduled. | `neon:compute_spec.rs:462-472`; `lbc:` §1 [live 11-10]; `lb:…/neon/spec.py:79` | Treat `read_only` as a **snapshot read-only endpoint**, with freshness equal to its snapshot time (§2.2). Serving does not rely on it for fresh reads. |
| **Suspend and wake.** Endpoints suspend when idle unless `suspend_timeout_seconds: -1`. **There is no wake-on-connect.** A suspended endpoint gives a connection error, not a hang; whether refused or reset is unmeasured **[GK]**. Resume is `POST /v1/projects/{p}/endpoints/{e}/start` or SDK `Client.ensure_running`, which starts and polls. Restarts still happen on redeploys and host restarts, even with `-1`. | ADR-0005; `lb:docs/api/lakebase-api.yaml:144,306`; `lb:clients/python/src/lakebase/client.py:117-140`; `lbc:` §2 | A wake path with bounded retry is mandatory (§2.2). |
| **Timings.** Resume takes **2.3–5.2 s**. A branch at head, LSN or timestamp is ready in seconds. Reading a branch needs its own endpoint (one compute). | `lbc:` §2 [live 11-10], §4 | Sizes the wake bound. Branch reads are phase 3. Read the primary after the load commits instead. |
| **Freshness.** `_lakebase_sync_state` (`target_table` PK, Iceberg `snapshot_id`, `synced_at`) is written only by `lakebase-atlas sync-table`, a full refresh in one transaction. It is unit-tested only and has never run live. It is created in the session `search_path`, and the swap's `DROP TABLE` fails when views depend on the target. No API exposes LSN or lag. | `lbc:` §3; `lb:…/synced_tables.py:107-160` | Freshness comes from data-level markers, not LSN: sync state when present, otherwise a configured `freshness_sql`. |
| **Discovery API.** Projects, branches, endpoints (`host`, `port`, `state`, `type`, `size`), roles, databases and `connection_uri`. Token scopes are `org`, `project`, `agent` and `admin`. | `lb:docs/api/lakebase-api.yaml:22-195`; `lbc:` §5 | API discovery comes later. API-issued role credentials are never used for serving traffic. |
| **Federation.** A StarRocks JDBC catalog over endpoints. | `lb:docs/architecture.md:133`; `lb:…/lakebase_atlas/starrocks.py:38-80` | Cross-source joins stay outside Serving. |
| **Readiness.** Verified on GCP only. Not built or not verified on-prem: read-only streaming, SeaweedFS on 3+ hosts, air-gapped install, project-token issuance, monitoring, a second pageserver, runbooks. ADR-0007 limits the pilot to synthetic data. | `lbc:` §6–7 | **An on-prem Lakebase at CBN next week is not credible.** The `lakebase` kind is for the GCP pilot and later. CBN serves from `postgres`. |
| **Cold cache.** After resume the local file cache is empty, so the first reads fetch pages from the pageserver or bucket. | **[GK]**, not covered by `lbc:` | Tracked by the cold-start histogram. |

## 2. Sources

### 2.1 Shared Postgres-wire layer (both kinds)

- **Adapter and pool.**
  - One `DBAPIAdapter` path serves both kinds. The pool class is renamed `PostgresPool`, and `LakebasePool` stays as an import alias.
  - `is_postgres_wire(kind)` (`postgres` or `lakebase`) replaces today's read-side `== "lakebase"` checks at `router.py:402,444`, `enforce.py:154,167`, `service.py:374`, and in `small_scan_order`.
- **Credentials.**
  - Exactly one of `dsn_env` (today's key) or `dsn_file`, read through `read_secret_file` with its 0600 check (`server/config.py:207`).
  - Literal `dsn` or `password` keys are rejected.
  - A DSN never appears in errors, logs or `/readyz` (`_safe_label`, `server/config.py:37`).
- **Accounts.**
  - `accounts.<workload>` with names from `VALID_WORKLOADS`, as for StarRocks. `interactive` is required whenever `accounts` is set, and a top-level DSN serves every workload.
  - There is one pool per distinct credential.
  - The flat pool keys stay as they are today (`server/config.py:1116-1135`).
- **Writability.**
  - `kind: postgres` uses `mode: read_only|read_write`. The default is **`read_only`**, since a new kind carries no compatibility debt.
  - `kind: lakebase` uses `endpoint_type` with the same values. The default is `read_write`, matching the Lakebase API and today's behaviour.
  - A read-only source runs with session `default_transaction_read_only=on` and opens every transaction with `BEGIN READ ONLY`. The router never sends it writes.
  - A write whose tables live only on read-only sources is rejected before any connection is opened. The write rule becomes "writes go only to writable Postgres-wire sources".
- **Session hardening (`session:` block).** Without the block, nothing changes and `validate` warns.
  - With it, connect passes these libpq `options`:
    - `-c default_transaction_read_only=on` (read-only sources);
    - `statement_timeout`;
    - `lock_timeout`;
    - `idle_in_transaction_session_timeout`;
    - `search_path=pg_catalog`.

    It also sets `application_name`.
  - `SHOW` checks verify the settings. On a mismatch the connection is closed, so the source fails closed. Lakebase's L4 router passes startup options through (`lbc:` §1).
    - PgBouncer in transaction mode drops or rejects most startup `options` **[GK]**. Behind a pooler, set `startup_options: false` and back it with role-level `ALTER ROLE … SET`, as the pilot's `wp09_serving` role does (`lbc:` §1).
  - `startup_options: false` switches to `SET` statements after connect, verified the same way.
  - Each query then runs `BEGIN [READ ONLY]`, then `SET LOCAL statement_timeout = min(request, cap)`, then `SELECT set_config('application_name', %s, true)` with the value `seeknal:<workload>:<audit_id>`.
- **Role guard** (both kinds; `role_guard: true` by default).
  - The pool refuses to start if `current_user` has `rolsuper` or `rolbypassrls`, because Serving's row filters assume RLS cannot be bypassed.
  - For `lakebase` it also refuses membership in `neon_superuser` (`pg_has_role(current_user,'neon_superuser','MEMBER')`), which catches API-created roles (ADR-0012, `lbc:` §1, §8).
  - On `postgres`, `role_guard: false` turns it off, with a `validate` warning. On `lakebase` it cannot be turned off.
- **TLS:** for `postgres`, `sslmode` defaults to `verify-full` when `session:` is present, and `sslrootcert_file` sets the CA. For `lakebase`, `verify-full` with a CA file is mandatory (§2.2).

### 2.2 Lakebase-specific (`kind: lakebase` with a `lakebase:` block)

```yaml
lakebase:
  project_id: prj_xxxxxxxxxxxxxxxxxxxx      # Lakebase IDs; audit + /readyz labels, never routing
  branch_id:  br_xxxxxxxxxxxxxxxxxxxx
  endpoint_id: ep_xxxxxxxxxxxxxxxxxxxx
  endpoint_type: read_write                 # read_only | read_write (default read_write); see "read-only endpoints"
  cold_start:
    connect_timeout_seconds: 5              # lbc: 5-10 s
    wake: api                               # api (MVP, S2) | none
    api_url: https://lakebase-cp.internal.example   # control plane; no secret here
    api_token_env: LB_OPS_PROJECT_TOKEN     # token scoped project:<id>; starts computes, nothing else
    max_query_wait_seconds: 10              # one query never waits longer (and never past its deadline)
    max_wake_seconds: 60                    # the background wake keeps trying this long (lbc: ~60 s)
    retry_backoff_ms: [500, 1000, 2000, 4000]
  freshness:
    sync_state_table: public._lakebase_sync_state   # default ON for lakebase; qualified read
    freshness_sql: null                     # optional, e.g. "select max(as_of_date) from stg.load_control"
```

- **Storage class:** always `object_storage`; setting it is a config error. WAL and page layers both reach the bucket and also sit on local disks (`lbc:` §6).
- **TLS:** `sslmode` must be `verify-full` and `sslrootcert_file` is required. Anything weaker is a config error, because plaintext is refused anyway (`lbc:` §1).
- **Credentials.** The DSN must belong to a SQL-created role, enforced by the role guard in §2.1. API-issued role credentials are never used.
  - A `project:<id>` API token is used **only** to start the endpoint.
  - Issuing such a token has not been exercised yet (`lbc:` §7).
- **Cold start and wake (MVP).** A suspended or restarting endpoint returns a connection error (`lbc:` §2). On a connect failure:
  1. The first request starts a **single-flight background wake** per endpoint: `ensure_running` semantics, i.e. `POST /v1/projects/{p}/endpoints/{e}/start`, then poll the endpoint `state` until `running` (`lb:clients/python/src/lakebase/client.py:117-140`). It is implemented on the already-locked httpx; the SDK is not a dependency.
  2. Connects are retried with backoff for up to `max_wake_seconds`.
  3. Each request waits at most `max_query_wait_seconds` and never past its own deadline. If it runs out, it gets the new F code `engine_starting` (503) with `Retry-After` and the next step "The database is waking up; retry in a few seconds."
  4. If the wake fails, or `max_wake_seconds` is exhausted, the engine becomes `unavailable` and later requests get `engine_unavailable`.
  5. Outages are logged separately from query errors (`lbc:` §8).

  `wake: none` keeps only the bounded retry. **Recommendation:** set `suspend_timeout_seconds: -1` on serving endpoints. The retry path stays mandatory, because redeploys and host restarts still happen (`lbc:` §2).
- **Read-only endpoints are snapshot endpoints** until Lakebase streams WAL to them (`lbc:` §1).
  - A `read_only` source reports freshness `max_lag = now − pg_postmaster_start_time()`, measured by the adapter itself (client SQL never runs `pg_*`), whenever no sync-state or `freshness_sql` marker exists.
  - `validate` warns: "read_only Lakebase endpoints serve a frozen snapshot".
  - The default stays `read_write`, which is the primary. A single-transaction load is consistent to read after it commits (`lbc:` §4).
- **Readiness.** D's `/readyz` reports `ok`, `starting` or `unavailable` per engine (a bounded enum). `starting` means a wake is in flight.
- **Metrics (bounded labels):**
  - `serving_lakebase_wake_total{engine,outcome}`
  - `serving_lakebase_connect_retries_total{engine}`
  - `serving_lakebase_cold_start_seconds{engine}`
  - `serving_lakebase_state{engine,state}`
- **Pool budget.** E warns when `instances × pool_max` exceeds the endpoint's `max_connections = max(25, CU×100)`. `cu` is declared in the block (`lbc:` §1).
- **Later:**
  - read-only endpoint lag via `pg_last_xact_replay_timestamp()`, once streaming lands;
  - branch- or `Static(Lsn)`-pinned D-1 snapshot reads (phase 3; one compute each);
  - endpoint `host:port` and state discovery through the Lakebase API;
  - JWT/OIDC login when Lakebase ships it;
  - dropping the wake path once wake-on-connect exists.

### 2.3 Storage class and the PII placement rule (extends E)

| Engine | `storage_class` | Rationale |
|---|---|---|
| `postgres` | `local` by default; can be declared `object_storage` if its data directory sits on S3-backed storage | Plain Postgres on local or attached block storage. |
| `lakebase` with a block | `object_storage`, fixed | WAL and page layers are offloaded to the bucket, and copies also sit on safekeeper, pageserver and compute local disks. No supported setup keeps data off object storage (`lbc:` §6). |
| legacy `lakebase` (no block) | `unknown`, treated as `object_storage` by the rule; fix it by declaring `storage_class` or relabelling | Fail closed: Serving cannot know which backend this is. |
| `duckdb` `iceberg_direct` | `object_storage`, fixed | Reads Iceberg data files from S3. |
| `duckdb` `local_file`, including B's accel cache built from the lake | `local` | The served bytes rest in a local file. B's loader already writes PII columns as typed NULL. |
| `starrocks` | must be declared; `object_storage` if omitted | Shared-data mode and Iceberg external catalogs are S3; shared-nothing is local **[GK]**. Fail closed. |
| `cube` | must be declared; `object_storage` if omitted | Cube Store pre-aggregations may sit in S3 **[GK]**. Fail closed. |

- **The rule (startup check, exit 2, one message per violation).**
  - A `pii: true` table's single serve (E's existing rule) must be on an engine whose effective `storage_class` is `local`.
  - The only exception is an engine listed in the new placement policy key `pii_object_storage_engines: [<engine>]`, an explicit and auditable exception.
  - E's existing `pii_engines` rule for DuckDB still applies alongside it.
- **Example message:**

  > `table 'cbn.src_pii.customer' is pii: true but its serve engine 'lb_ops' (kind lakebase) has storage_class object_storage; CBN policy forbids PII in object storage — serve it from a postgres source or list the engine in policy.pii_object_storage_engines`
- **Discovery** applies the rule while building placement. A `pii` schema configured on an `object_storage` source refuses at startup. At refresh time the refresh fails and the old catalog is kept.
- **Tokenized tables** (§9, `tokenized: true`) hold no plaintext and are exempt.
- **Offline validation.** `python -m seeknal.serving validate` checks the rule offline, since it needs only the config and placement.

### 2.4 Backward compatibility and migration

- **Legacy configs.** `kind: lakebase` without a block parses and behaves byte-identically to today: writes, sync-state discovery and `LakebasePool` all work as before. It warns once at startup and in `validate`:

  > `connection 'lb' uses kind: lakebase without a lakebase: endpoint block; if this is a plain PostgreSQL database relabel it kind: postgres (mode: read_write keeps today's write behaviour), otherwise add the lakebase: block`
- **Migration note in the docs.** The CBN lab's `lb` becomes `kind: postgres`, plus `mode: read_write` only if writes are still needed. Physical names and placement keys are unchanged.
- **Audit.**
  - `engine_kind` reports the configured kind, so legacy configs still log `lakebase`.
  - New fields: `storage_class`, and for Lakebase only, `lakebase_endpoint` IDs and `cold_start`.
- **Metrics.**
  - The shared layer uses `serving_postgres_pool_{in_use,idle}{engine,kind,account}` and `serving_sync_lag_seconds{engine}`.
  - If P0 (D) ships pool metrics named after Lakebase, they stay as deprecated duplicates for one release, documented.
- **Discovery config.**
  - The existing `placement.discovery.lakebase: {engine, synced_tables}` stays valid and still names the sync-state source.
  - The new introspection block is `postgres_sources` (§3), valid for both kinds.

### 2.5 Placement mapping and freshness

- **Names.**
  - The physical name is `<schema>.<table>`.
  - The logical name is `<logical_catalog>.<schema>.<table>`. `logical_catalog` defaults to the source name, and `schema_map` renames schemas.
  - Only identifiers matching `^[a-z_][a-z0-9_]{0,62}$` are exposed, which keeps the rewrite at `router.py:214` exact.
- **Merging with the lake.**
  - A Postgres-wire table and a Lakekeeper table with the same logical name become one entry with two serves.
  - The columns must be identical. Otherwise the wire serve is dropped with a warning.
  - `pii` tables never merge, and the lake serve is dropped.
- **Freshness uses data-level markers, not LSN** (`lbc:` §3). The `freshness:` block works for both kinds. Sources are tried in order:
  1. **`_lakebase_sync_state` row.** The row means "equals Iceberg snapshot N as of `synced_at`". Lag is 0 when `snapshot_id` matches the current Iceberg snapshot, otherwise `now − synced_at`. `refresh_interval_seconds` is added in either case (`build.py:188-204`). On by default for `lakebase`; opt-in for `postgres`.
  2. **`freshness_sql`.** A config-reviewed, single-value query, run by discovery in a read-only transaction. For example, the CBN mart's `select max(as_of_date) from stg.load_control` gives lag = `now − value`. It is source-wide and opt-in.
  3. **Read-only Lakebase snapshot age** (§2.2).
  4. **None of these:** `max_lag_seconds: None`, meaning live operational data.

  Caveats:
  - `sync-table` has never run live (`lbc:` §3).
  - CBN's pipeline dual-writes its report tables into the `postgres` report DB, so it must upsert the same row, or rely on `freshness_sql`.
  - Serving reads the table schema-qualified, because the pinned `search_path` breaks the unqualified query at `discovery/lakebase.py:34`. `sync-table` also creates the table in its own session `search_path`, so the qualified name must be configured to match.

### 2.6 YAML: full CBN example

```yaml
secrets_env_file: /etc/seeknal/secrets/serving.env       # mode 0600; *_DSN hold libpq URIs
default_catalog: cbn
connections:
  pg_report:                          # PII report DB: rpt_* copies + src_pii customer replica
    kind: postgres                    # storage_class: local (default)
    mode: read_only                   # default for kind postgres
    accounts:
      interactive: {dsn_env: CBN_PG_REPORT_DSN_BI}
      bi:          {dsn_env: CBN_PG_REPORT_DSN_BI}          # same credential -> same pool
      report:      {dsn_env: CBN_PG_REPORT_DSN_BI}
      ask:         {dsn_file: /etc/seeknal/secrets/pg-report-ask.dsn}   # role without PII grants
    pool_max: 12
    pool_min: 2
    acquire_timeout_seconds: 5
    connect_timeout_seconds: 5
    session:
      statement_timeout_ms: 30000
      lock_timeout_ms: 2000
      idle_in_transaction_session_timeout_ms: 10000
      search_path: pg_catalog
      application_name: seeknal-serving
      startup_options: true
      connection_limit: 30
    tls: {sslmode: verify-full, sslrootcert_file: /etc/seeknal/tls/cbn-pg-ca.pem}
  lb_ops:                             # OPTIONAL (GCP pilot today): Atlas Lakebase, non-PII operational data
    kind: lakebase                    # storage_class: object_storage (fixed)
    dsn_env: CBN_LB_OPS_DSN           # a SQL-created role; the role guard refuses neon_superuser members
    pool_max: 4
    session: {statement_timeout_ms: 15000, lock_timeout_ms: 2000,
              idle_in_transaction_session_timeout_ms: 10000, search_path: pg_catalog}
    tls: {sslmode: verify-full, sslrootcert_file: /etc/seeknal/tls/lakebase-ca.pem}   # verify-full required
    lakebase:
      project_id: prj_xxxxxxxxxxxxxxxxxxxx
      branch_id:  br_xxxxxxxxxxxxxxxxxxxx
      endpoint_id: ep_xxxxxxxxxxxxxxxxxxxx
      endpoint_type: read_write       # read_only endpoints are frozen snapshots today
      cu: 1                           # max_connections = max(25, CU x 100)
      cold_start: {wake: api, api_url: https://lakebase-cp.internal.example, api_token_env: LB_OPS_PROJECT_TOKEN,
                   connect_timeout_seconds: 5, max_query_wait_seconds: 10, max_wake_seconds: 60}
  lake:
    kind: duckdb
    profile: iceberg_direct           # storage_class: object_storage (fixed); unchanged config
    extension_directory: /opt/seeknal/duckdb_extensions
    extensions: [httpfs, iceberg]
    allowed_s3_prefixes: [s3://cbn-warehouse/gold/]
    recycle_seconds: 240
    iceberg_catalogs:
      lake: {endpoint: https://lakekeeper.cbn.internal.example/catalog, warehouse: cbn,
             token_uri: https://kc.cbn.internal.example/realms/cbn/protocol/openid-connect/token,
             client_id_env: LK_CLIENT_ID, client_secret_env: LK_CLIENT_SECRET, access_delegation: vended,
             s3: {endpoint: s3.cbn.internal.example:9000, region: local, key_id_env: S3_KEY_ID, secret_env: S3_SECRET}}
placement:
  discovery:
    default_catalog: cbn
    refresh_interval_seconds: 300
    lakekeeper: {url: https://lakekeeper.cbn.internal.example/catalog, warehouse: cbn,
                 oauth: {token_url: ..., client_id: seeknal-serving-discovery, client_secret_env: LK_CLIENT_SECRET},
                 namespaces: [rpt]}
    engines:
      lake:      {kind: duckdb, dialect: duckdb, profile: iceberg_direct, alias: lake}
      pg_report: {kind: postgres, dialect: postgres}
      lb_ops:    {kind: lakebase, dialect: postgres}
    postgres_sources:                  # NEW: pg_catalog introspection, valid for postgres and lakebase
      pg_report:
        account: interactive
        logical_catalog: cbn           # rpt_* merge with the Lakekeeper rpt_* (same logical names)
        freshness:                     # opt-in for postgres: D-1 copy freshness
          sync_state_table: public._lakebase_sync_state
          freshness_sql: "select max(as_of_date) from stg.load_control"
        schemas:
          rpt:     {include: ["rpt_*"], exclude: ["rpt_*_stg"]}
          src_pii: {include: [customer], pii: true}
        tables:
          cbn.src_pii.customer:
            access: {pattern: point, keys: [customer_id], filterable: [customer_id, msisdn_hash], max_rows: 50}
          cbn.rpt.rpt_txn_detail:
            access: {pattern: filter, filterable: [customer_id, as_of_date, branch_code],
                     range_filterable: [as_of_date], sortable: [as_of_date, txn_id], max_rows: 5000}
      lb_ops:
        account: interactive
        logical_catalog: ops
        schemas:
          ops: {include: ["ticket_*", "case_status"]}     # non-PII only
    policy:
      require_filter_min_rows: 100000
      max_in_list: 1000
      max_offset: 10000
      guard_mode: enforce
      pii_engines: []                  # E: no PII on DuckDB
      pii_object_storage_engines: []   # NEW: no PII on object storage (CBN rule)
```

**Validation check** (an oracle test in G):
- Moving `src_pii` under `postgres_sources.lb_ops.schemas`, or statically placing `cbn.src_pii.customer` on `serve: {lb_ops: …}`, refuses startup (exit 2) with the §2.3 message.
- The same table on `pg_report` passes: storage class `local`, single serve.
- On `lake` it fails twice: E's DuckDB rule and the object-storage rule.

## 3. Discovery for both kinds (pg_catalog introspection, not built)

`discovery/postgres_introspect.py` runs four read-only queries per refresh on the introspection account. Each is parameterised by the allowlisted schemas (`n.nspname = ANY(%s)`).

1. **Relations:** `pg_class` joined to `pg_namespace`, `relkind IN ('r','p','v','m')`, filtered by `has_table_privilege(c.oid,'SELECT')`. `reltuples` of -1 becomes `row_count: None` **[GK, PG14+]**.
2. **Columns:** `pg_attribute` (`attnum > 0`, not dropped, `has_column_privilege`), with `format_type` and `attnotnull`.
3. **Indexes:** `pg_index` with `indisvalid AND indisready`, reading primary, unique, `indkey`, `pg_am.amname`, opclasses, whether the key is an expression, and any partial predicate.
4. **Server facts:** `SHOW server_version_num` and `SHOW default_transaction_read_only`. The latter must be `on` for a read-only source, otherwise the source fails closed.

| Field | Derived from |
|---|---|
| `keys` | The primary key. Failing that, the narrowest unique, non-partial index whose columns are all NOT NULL. |
| `filterable` | The leading column of a valid, non-partial, non-expression btree or hash index, plus the primary key. |
| `range_filterable` | The leading btree column. |
| `prefix_filterable` | A leading btree column using `text_pattern_ops`/`varchar_pattern_ops` or `COLLATE "C"` **[GK]**. |
| `sortable` | NOT NULL columns that form a btree prefix, plus the primary key. |
| `pattern` | For `pii` tables: `point` when keys exist, else `filter`. Otherwise `scan` when `row_count < require_filter_min_rows`, else `filter`. Views and unknown row counts get `filter`. |
| `require_filter` | True for `point` and `filter`. |
| `max_rows` | 50 for `pii` tables. |

- **Partial and expression indexes** are reported but not used for derivation; columns that rely on them must be declared. A declared column with no supporting index produces a warning.
- **Deny by default.**
  - A table is exposed only when its schema is listed, it passes include/exclude and the identifier regex, and the account has the privilege. Columns additionally need column privilege.
  - An override naming a table that was not found refuses at startup. At refresh it fails that refresh, and the old catalog is kept.
- **The storage rule (§2.3)** is enforced while placement is built.
- **Refresh** runs at startup and then on the configured interval.
  - `POST /refresh` on the ops listener is phase 2.
  - `python -m seeknal.serving.discovery --config X --emit placement.yaml` writes a reviewable static placement, which is the go-live fallback.
- **Lakebase-specific discovery** (projects, branches, endpoints via `lb:docs/api/lakebase-api.yaml`) is optional and comes later. The MVP takes endpoint IDs from config.
- **E extensions:**
  - every column named in `access.*` exists;
  - a `point` table has `keys`;
  - `sortable` columns are NOT NULL;
  - PII on a writable source warns;
  - the storage rule.

## 4. Storage per pattern, guardrails and routing

### 4.1 Which store

| Pattern | Store | Why |
|---|---|---|
| `point` (≤100 keys) | B-tree primary key or unique index on a Postgres-wire source | An O(log n) probe; MVCC readers don't block each other. The pool removes the 21.8 ms connect measured on the lab. |
| `filter` | B-tree on a Postgres-wire source: composite `(filter_col, sort_col)` for keyset, `text_pattern_ops` for prefix, BRIN for append-only dates **[GK]** | An index range probe. `(a,pk) > ($1,$2)` uses the composite index **[GK]**. |
| `scan` / aggregate | DuckDB accel file or `iceberg_direct`; StarRocks | Columnar and vectorised. |
| semantic | Cube | Already routed (`router.py:414-417`). |

- **`postgres` versus `lakebase`.** Both are fine when warm.
  - `lakebase` adds cold start (resume 2.3–5.2 s, `lbc:` §2) and cold-cache page fetches **[GK]**.
  - Its `read_only` endpoints are frozen snapshots today.
  - So latency-critical point and filter serving prefers `postgres`, or an always-on (`suspend_timeout_seconds: -1`) `read_write` Lakebase endpoint.
  - PII at CBN is always `postgres` (§2.3).
- **DuckDB is wrong for concurrent point lookups.**
  - Its ART indexes cover only PK and UNIQUE; other filters rely on zone maps **[GK]**.
  - Serving bounds it with `max_concurrent_cursors` (32, `server/config.py:417`).
  - `iceberg_direct` adds an object-store manifest read to each query **[GK]**.

### 4.2 Guardrails (`guardrails.py`, not built)

Guardrails run after routing and before `enforce`, on the client statement, so grant row filters never count as the user's filter. They apply when the chosen engine is a Postgres-wire source, except `max_rows`, which applies to every engine.

1. **Sargable conjuncts.** These are the top-level `AND` conjuncts of the form `col <op> literal|$n`:
   - `=`;
   - `IN` with at most `max_in_list` items;
   - range comparisons and `BETWEEN` on `range_filterable` columns;
   - `LIKE 'p%'` on `prefix_filterable` columns;
   - an `OR` of equalities on one column.

   A column wrapped in a function or CAST doesn't count (that is CBN's `CAST(p.id AS varchar)` defect), and neither does `IS [NOT] NULL`.
2. **`require_filter`.** The query needs one of:
   - a sargable conjunct on a `filterable` column;
   - for a `point` table, every key constrained, with key product ≤ `point_lookup_max_keys`;
   - a same-source equi-join onto a `filterable` column from a table that is itself filtered.

   Otherwise it is rejected with `filter_required` (422).
3. **Derived tables.** One level of plain-projection derived table (Superset virtual datasets) is transparent. Deeper nesting returns `filter_required`.
4. **ORDER BY** on a `point` or `filter` table must use `sortable` columns, otherwise `sort_not_indexed` (422). Queries with GROUP BY are exempt.
5. **OFFSET** above `max_offset` returns `offset_too_large` (422).
6. **Row cap.** `max_rows = min(grant, request, access.max_rows of every table)`. F's `row_cap_hit` notice is kept.
7. **Timeouts.** The session caps from §2.1 apply. On `lakebase`, the cold-start wait counts against the same deadline.
8. **`guard_mode: warn`** is for rollout only.
9. **EXPLAIN ceiling** (`query_too_expensive`): phase 2.

### 4.3 Router

- **Rule order:**
  1. hint
  2. write (writable Postgres-wire only)
  3. semantic
  4. `point_lookup` (Postgres-wire)
  5. **`filter`** (new)
  6. analytics
  7. small_scan
- **The `filter` rule** fires for a SELECT with no aggregate, window or DISTINCT, when one Postgres-wire source serves every referenced table and some sargable conjunct is on a `filterable` column. Ties go to `postgres` before `lakebase`, because warm local storage wins, then to placement order.
- **Aggregates** stay on `analytics_order`.
- **`features.py`** gains `predicates`, `offset` and `order_by`.

## 5. Governed select API (`POST /v1/select`, not built)

```json
{"table": "cbn.rpt.rpt_txn_detail",
 "columns": ["txn_id", "as_of_date", "branch_code", "amount"],
 "where": {"_and": [{"customer_id": {"_eq": "C-1001"}},
                    {"as_of_date": {"_gte": "2026-10-01", "_lt": "2026-11-01"}},
                    {"_not": {"branch_code": {"_in": ["X01", "X02"]}}}]},
 "order_by": [{"as_of_date": "desc"}, {"txn_id": "desc"}],
 "limit": 100, "after": "<opaque cursor or omitted>"}
```

- **Request rules.**
  - Operators: `_eq`, `_neq`, `_in`, `_nin`, `_gt`, `_gte`, `_lt`, `_lte`, `_like` (prefix only, `^([^%_\\]|\\[%_\\])*%$`), `_is_null`, `_and`, `_or`, `_not`.
  - Depth is at most 5 and the tree at most 64 nodes.
  - Values follow `seeknal-params-v1`, which rejects floats.
  - Anything else returns `select_invalid` (400).
- **Compilation.** Identifiers come only from placement and schemas, and every value becomes a `$n` parameter. The result then runs through the unchanged pipeline: route → guardrails → `enforce` → rewrite → render.
- **Keyset pagination.**
  - Ordering uses `sortable` columns in a single direction, with the visible primary key appended.
  - Serving fetches `limit+1` rows and returns a `next` cursor: base64url of `{v,t,o,k}`, HMAC-SHA256 signed with the key from `cursor_key_env`.
  - A bad cursor returns `cursor_invalid` (400).
- **Grant.**
  - Claims: `sql_sha256` = SHA-256 of the exact body bytes, `api: "select.v1"`, `sql_dialect: postgres`, and no `params_sha256`.
  - `/v1/query` rejects any `api` claim; `/v1/select` requires one.
  - Each page needs its own grant, because the grant's `jti` is single-use.
  - `enforce` takes the body as its integrity input; `enforce.py` adds only the claim.
  - **Issuer dependency:** the grant issuer must mint these grants, which is outside this run.
- **Response:** the `/v1/query` body plus `next`.
- **GraphQL comes later** as a facade over this request object (012). There is no consumer at CBN yet, and it would need per-grant schemas, cost limits, relationships and a new library in the image.

## 6. Cube-inspired: now and later

| Item | Decision |
|---|---|
| Named sources across several drivers | **Now** (`connections`, D2). |
| Security context driving row filters | **Already built.** |
| Per-table access metadata | **Now.** |
| Declared measures and dimensions; `aggregate` in select | **Phase 2.** |
| Rollup routing to the DuckDB accel cache | **Phase 3.** |
| Semantic modelling, join graph, caching | **Never in Serving.** Cube stays behind `kind: cube`. |

## 7. Joins

- **Same-source joins,** including a `postgres`↔`postgres` join on one source, work through feasibility under the §4.2 rule.
- **Cross-source joins** (`pg_report`↔`lb_ops`, `pg_report`↔`lake`) return `query_spans_engines` (422). With PII, E's `pii_cross_engine` fires first, even with a hint.
- **Federation** across Lakebase and the lake belongs to the StarRocks JDBC catalog (011).
- **Relationships (010)** come in phase 2.

## 8. Security

- **SQL injection is impossible by construction.**
  - Identifiers come only from the allowlist and the identifier regex.
  - SQL goes parse → AST → render, behind the round-trip guard (`service.py:74`).
  - Values travel only as parameters **[GK: psycopg2 adaptation]**.
  - The adapter's own `SET`s use computed integers, and `set_config(%s)` takes `application_name` as a parameter.
  - The `pg_*`, `set_config`, `dblink` and `lo_*` bans stay (`enforce.py:171-185`).
- **Masks** run on the Postgres-wire source. Masked columns cannot be filtered or sorted on.
- **PII protections:**
  - E's single-serve rule;
  - the **storage-class rule**;
  - no merge with lake tables;
  - forced `point`/`filter` pattern with `max_rows: 50`;
  - the `ask` workload's account has no PII grants.
- **Audit (C) gains these fields:**
  - `api`;
  - `engine`, `engine_kind`, `storage_class`;
  - `account`;
  - `lakebase_endpoint` IDs and `cold_start` (Lakebase only);
  - `access_pattern`;
  - `guard`;
  - `filter_columns` (names only);
  - `effective_max_rows`;
  - `paged`.

  Audit records never contain values or SQL text.
- **Metrics (D) gain these series**, all with bounded labels:
  - `serving_guard_rejections_total{code}`;
  - `serving_postgres_pool_*`;
  - `serving_sync_lag_seconds`;
  - `serving_discovery_refresh_total{engine,outcome}`;
  - the `serving_lakebase_*` series from §2.2.
- **Least-privilege SQL for CBN's DBA** (the `postgres` report DB). Run it as the database owner. Set passwords with `\password`. Table names are placeholders.

```sql
CREATE ROLE seeknal_read_rpt NOLOGIN;
CREATE ROLE seeknal_read_pii NOLOGIN;
CREATE ROLE seeknal_bi  LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
       CONNECTION LIMIT 30 IN ROLE seeknal_read_rpt, seeknal_read_pii;
CREATE ROLE seeknal_ask LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
       CONNECTION LIMIT 10 IN ROLE seeknal_read_rpt;
ALTER ROLE seeknal_bi SET default_transaction_read_only = on;   -- repeat these five for seeknal_ask
ALTER ROLE seeknal_bi SET statement_timeout = '30s';
ALTER ROLE seeknal_bi SET lock_timeout = '2s';
ALTER ROLE seeknal_bi SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE seeknal_bi SET search_path = pg_catalog;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT CONNECT ON DATABASE cbn_report TO seeknal_read_rpt, seeknal_read_pii;
GRANT USAGE ON SCHEMA public TO seeknal_read_rpt;
GRANT SELECT ON public._lakebase_sync_state TO seeknal_read_rpt;      -- freshness
GRANT USAGE ON SCHEMA rpt TO seeknal_read_rpt;
GRANT SELECT ON rpt.rpt_txn_detail, rpt.rpt_daily_branch /* , ...explicit list */ TO seeknal_read_rpt;
GRANT USAGE ON SCHEMA src_pii TO seeknal_read_pii;
GRANT SELECT (customer_id, full_name, msisdn_hash, segment, updated_at) ON src_pii.customer TO seeknal_read_pii;
CREATE INDEX CONCURRENTLY ON rpt.rpt_txn_detail (customer_id, as_of_date, txn_id);
CREATE INDEX CONCURRENTLY ON rpt.rpt_txn_detail (as_of_date, txn_id);
ANALYZE rpt.rpt_txn_detail;   -- the pipeline runs ANALYZE after every load
```

For `lb_ops`, run the same role pattern **in SQL**, connected as the project owner.
- Do not create Serving's role through the Lakebase API. API roles are `neon_superuser` members with BYPASSRLS (`lb:docs/decisions/0012-…md`, `lbc:` §1), and the role guard refuses them.
- Lakebase also lacks TLS on the compute↔pageserver/safekeeper hop in the pilot (`lbc:` §6). That is another reason it is not a PII store.

## 9. safeData: tokens in the lake, reveal at result time (designed extension point)

**Goal.** CBN's hard rule is no PII in the lake. safeData would tokenize PII at ingestion with domain-scoped deterministic tokens, so Iceberg and DuckDB hold only tokens, and Serving would reveal plaintext for authorized users after execution.

**safeData readiness** (`safe:` §1, §2, §6):
- **Built, on synthetic data:**
  - deterministic scoped tokens (`tok_` plus 32 hex) and shared join domains;
  - normalisation;
  - batch ingest of 100 values;
  - a two-person reveal (request, then approval by a *different* subject, then detokenize) with TTL ≤ 900 s, ≤ 5 uses and ≤ 100 tokens;
  - an HMAC-chained audit.
- **Not built:**
  - Serving-side reveal;
  - partial reveal;
  - lookup-only tokenize (`ingest` mints tokens for new values);
  - erasure, KMS/HSM, HA and audit export;
  - the external review;
  - a built image.
- **Throughput:** about 30 ms per reveal flow, and about 76 flows/s at 16 clients. A 10,000-row report needs 100 flows, which takes seconds.

### 9.1 The hook

- **Where it runs.** In `http.py`, after `adapter.execute` and before the audit record and the response: `result = result_transform.apply(result, prepared.transform_plan, ctx)`. The plan is built at `prepare` time.
- **Per-column actions:**
  - `keep`, the default for token columns;
  - `reveal(domain)`;
  - `reveal(domain)+mask(m)`, which applies Serving's mask `m` in Python with `MASK_TEMPLATES` semantics (`enforce.py:187-213`). safeData has no partial reveal, so plaintext passes through Serving memory. Record that in the DPIA.
- **Eligibility.** Only bare top-level projections, the same rule as masks (`enforce.py:1247`). Tokens used in joins, GROUP BY or COUNT DISTINCT stay tokens, and those operations give the same results on tokens as on plaintext (`safe:` §1).
- **Policy.**
  - The grant decides: `TableGrant.reveal: {column: "full"|"last4"|"year"}`. The column must be visible; absence means `keep`.
  - The vault is a coarse second gate on service identity, domain and purpose (`safe:` §3).
- **Client.** `RevealClient.reveal(domain, tokens, *, subject, workload, audit_id, purpose, deadline)`.
  - Tokens are deduplicated and sent in batches of ≤ 100, with concurrency ≤ 8 and inside the query deadline.
  - `max_reveal_values` defaults to 1,000; above it the request fails with `reveal_limit_exceeded` (422).
  - `on_reveal_failure: refuse` (the default) returns `reveal_unavailable` (503) with no rows. `keep_token` returns the tokens plus a `reveal_skipped` notice.
  - Three implementations: `NoRevealClient`, `FakeRevealClient`, `SafeDataRevealClient`.
- **Vault flow: not decided.**
  - (a) Two service identities, a requester and an approver.
  - (b) Contract v1: the issuer also signs a short-lived ES256 decision with `aud: safe-data`, and Serving forwards it.

  **(b) is recommended.**
- **Audit:** `reveal: [{table, column, domain, values, outcome, vault_request_ids}]`. Counts and IDs only, never plaintext or tokens.
- **Metrics:** `serving_reveal_values_total{domain,outcome}` and `serving_reveal_latency_seconds{domain}`.

### 9.2 Token columns as metadata, and the storage rule

- **Declaration.** `columns: {msisdn: {token_domain: phone, normalization: digits-v1}}` in placement. E checks that the domain is configured and the normalisation matches.
- **Storage-class rule (§2.3).**
  - A `tokenized: true` table holds no plaintext, so it is **exempt** and may sit on object storage (the lake, DuckDB `iceberg_direct`, Lakebase).
  - A table that stores plaintext PII (`pii: true`) is still bound to `local` engines, whether or not some of its columns are also revealable.
  - The safeData vault holds encrypted plaintext. Its config declares `reveal.vault.storage_class`, and E refuses `object_storage` there unless `pii_object_storage_engines` lists `safedata_vault`. Today the vault is SQLite on local disk (`safe:` §5), so the value is `local`.
- **Search.**
  - An `=`/`IN` filter with a plaintext value is normalised, run through `tokenize(domain, values)`, and compared as token equality.
  - This **needs a lookup-only tokenize, which is not built.** Until it exists, the select API returns `token_search_unavailable`.
  - Range, prefix and LIKE filters on token columns are impossible by design and return `token_filter_unsupported` (422).
  - Plaintext search values travel only as parameters, so `/v1/select` is the search path.

### 9.3 What changes, and what stays on Postgres or Lakebase

- **In principle,** PII-detail reports could then run on the lake or DuckDB with no PII replica: joins run on tokens, and Serving reveals only the displayed columns.
- **Not next week:**
  - reveal latency on large reports;
  - plaintext passing through Serving for partial reveal;
  - the vault is a single node;
  - erasure and key custody are open.
- **What stays on Postgres-wire sources:**
  - OLTP and filter tables;
  - operational and near-real-time replicated data;
  - online-store tables;
  - plaintext prefix, partial or fuzzy search, which tokens cannot serve.

  At CBN the remaining PII search table stays a `postgres` source under the storage rule. The replica shrinks to the search columns, or disappears if search can be equality-only.
- **Next week:**
  - keep the `postgres` PII replica;
  - switch bronze to a keyed HMAC per domain;
  - pilot safeData alongside.

  Moving to safeData later means a coordinated re-tokenization backfill.

## 10. Scope

**MVP:**
- the shared Postgres-wire layer for both kinds, with accounts, session hardening and TLS;
- the Lakebase block:
  - endpoint metadata;
  - the `verify-full` requirement;
  - the role guard;
  - a single-flight wake through the control-plane start API, with bounded retry;
  - `engine_starting`;
  - snapshot freshness for read-only endpoints;
  - readiness and metrics;
- `freshness_sql` for both kinds;
- `storage_class` and the PII storage rule;
- legacy `lakebase` compatibility, with the deprecation warning;
- discovery for both kinds, with sync-state freshness;
- guardrails and the `filter` route;
- `/v1/select` with keyset cursors;
- six new F codes: `filter_required`, `sort_not_indexed`, `offset_too_large`, `select_invalid`, `cursor_invalid`, `engine_starting`;
- the live suite.

**Days.** Rev 3 adds about **0.5–0.75 factory day**:
- S2 runs in parallel and now includes the wake API call.
- There is one extra integration step.

The plan still fits **4** days, not 3.

**Cut order:**
1. S2's wake API, readiness and metrics. Fall back to `wake: none` with bounded retry. Keep the endpoint metadata, `verify-full`, the role guard, `storage_class` and legacy handling, which validation and safety need. CBN's Lakebase use is optional, non-PII, and on the GCP pilot only (`lbc:` §7).
2. Keyset `after`.
3. `/v1/select`.
4. The join and virtual-dataset guardrail rules.

**Later run R2: reveal hook (§9), about 2 days.**
- **Gate:**
  - safeData's review is done;
  - the reveal model is chosen;
  - lookup-only tokenize exists and the image is built;
  - the DPIA accepts plaintext transit.
- **Scope:** `result_transform.py`, `reveal_client.py`, `token_domain` and its E checks, audit and metrics, and 4 codes.

**Phase 2:**
- Lakebase read-only endpoint lag (once streaming lands), endpoint discovery via the API, and JWT login;
- EXPLAIN ceiling;
- partial and expression indexes;
- `POST /refresh`;
- measures in select;
- relationships (010);
- GraphQL (012);
- shared replay store;
- result cache (008).

**Phase 3:**
- rollups;
- branch or `Static(Lsn)` snapshot serving;
- federation (011);
- discovery through the Lakebase API.

## 11. Factory decomposition (MVP run on the P0 result)

**Run rules:**
- **Base:** P0's imported, verified commit.
- **Constraints:** no new dependencies, no network in tests, no secrets, no push.
- **Path names:** the factory drops any path containing the components `samples`, `sample`, `data`, `integration`, `models`, `reports`, `.env` or `.git`, so every path below avoids them.
- **Naming:** `postgres_*` for the shared wire layer, `lakebase_*` only for the Lakebase-specific parts.
- **Ownership:** one writer per shared file.
- **Integration order:** G (codes) → R → S1 → S2 → Q → J → G (rest).
- **S1 ↔ S2 interface, fixed in the spec.** S2 exposes:
  - `parse_lakebase_block(raw) -> LakebaseEndpointConfig`;
  - `effective_storage_class(conn_cfg) -> "local"|"object_storage"|"unknown"`;
  - `ColdStartConnector(connect_fn, cfg, metrics, http_client).connect(deadline)`;
  - `role_guard_sql(kind) -> str`;
  - `snapshot_freshness(conn, cfg) -> float | None`.

  S1 calls them from `server/config.py`, `server/app.py` and `adapters.py`.

| Sub-task | Writes (exclusive) | Acceptance |
|---|---|---|
| **S1: shared Postgres-wire sources** | `postgres_session.py`, `server/config.py`, `server/app.py`, `adapters.py` (`PostgresPool`, plus the `LakebasePool` alias), `docs/serving/postgres-sources.md`, `tests/serving/test_postgres_sources_config.py`, `tests/serving/test_postgres_session.py`, and the existing `test_server_config.py`, `test_lakebase_pool.py`, `test_adapters.py`, `test_server_app.py` | <ul><li>`kind: postgres` parses, with default `mode: read_only`.</li><li>Legacy `kind: lakebase` behaves byte-identically and warns once.</li><li>`dsn_env` and `dsn_file` are exclusive; literal secrets are rejected.</li><li>Accounts are per workload; a missing workload is refused. One pool per credential.</li><li>A read-only source gets session and transaction read-only, and writes are refused.</li><li>Connect kwargs carry options, `application_name` and TLS.</li><li>A `SHOW` mismatch fails closed. `set_config` is parameterised.</li><li>A planted DSN never appears.</li><li>The role guard refuses `rolsuper`/`rolbypassrls` by default; `role_guard: false` works on `postgres` with a warning and is refused on `lakebase`.</li><li>The app wires `postgres_connects` into discovery.</li></ul> |
| **S2: Lakebase-specific** | `lakebase_endpoint.py`, `docs/serving/lakebase-sources.md`, `tests/serving/test_lakebase_endpoint.py` | <ul><li>The block parses. IDs are validated (`prj_`/`br_`/`ep_` + 20 base32 characters, `lb:docs/architecture.md:48`).</li><li>`endpoint_type` defaults to `read_write`.</li><li>`storage_class` is fixed; setting it is an error.</li><li>`effective_storage_class`: Lakebase with a block → `object_storage`, legacy → `unknown`, `postgres` → `local` or as declared.</li><li>`sslmode` weaker than `verify-full`, or a missing CA, is refused.</li><li>The role guard refuses `rolsuper`, `rolbypassrls` and `neon_superuser` members.</li><li>Wake (against a fake control plane): a connect failure triggers one `POST …/start` per endpoint, then polls `state`. Concurrent waiters share it.</li><li>Each query waits at most `max_query_wait_seconds` and never past its deadline, then gets `engine_starting` with `Retry-After`.</li><li>A failed wake or exhausted `max_wake_seconds` marks the engine `unavailable`.</li><li>The token is never logged.</li><li>`wake: none` keeps only the bounded retry.</li><li>Read-only snapshot freshness uses `pg_postmaster_start_time()`, run by the adapter.</li><li>Readiness reports `starting`; the metrics increment.</li><li>Sync state is on by default.</li></ul> |
| **Q: discovery** | `discovery/postgres_introspect.py`, `discovery/lakebase.py` (qualified sync-state read), `discovery/config.py`, `discovery/build.py`, `discovery/__main__.py`, `docs/serving/discovery.md`, `tests/serving/fake_postgres_catalog.py`, `tests/serving/test_discovery_postgres_introspect.py`, `tests/serving/test_discovery_merge.py`, and the existing `test_discovery_build.py`, `test_discovery_sources.py` | <ul><li>Queries are parameterised and work for both kinds.</li><li>The §3 rules hold, and nothing is exposed by default.</li><li>Merge: identical columns give two serves; a mismatch drops the wire serve with a warning; PII never merges.</li><li>The storage rule is enforced during build, so a `pii` schema on an object-storage source refuses.</li><li>Freshness falls through sync state, then `freshness_sql`, then read-only snapshot age, then `None`. `freshness_sql` runs in a read-only transaction and must return exactly one value.</li><li>The legacy `lakebase:` block still works.</li><li>`--emit` output loads as static placement.</li></ul> |
| **R: guardrails and routing** | `guardrails.py`, `placement.py`, `features.py`, `router.py`, `service.py` (the guard hook, and `prepare` split into `verify_grant` and `prepare_statement`), `docs/serving/access-patterns.md`, `tests/serving/test_access_guardrails.py`, `tests/serving/test_router_filter_rule.py`, and the existing `test_placement.py`, `test_router.py`, `test_service.py`, `test_features.py` | <ul><li>`TableAccess` is validated.</li><li>Every §4.2 rule has an accept case and a reject case.</li><li>`filter` prefers `postgres` over `lakebase`.</li><li>Aggregates on mirrored tables go to the lake.</li><li>Writes go only to writable Postgres-wire sources.</li><li>`max_rows` is the minimum of the caps.</li><li>`warn` counts without rejecting.</li><li>Existing routing tests pass unchanged.</li></ul> |
| **J: select API** | `select_api.py`, `keyset.py`, `http.py` (`/v1/select`), `enforce.py` (the `api` claim), `docs/serving/select-api.md`, `tests/serving/test_select_compiler.py`, `tests/serving/test_keyset_cursor.py`, `tests/serving/test_select_http.py`, and the existing `test_http.py`, `test_enforce.py` | <ul><li>Golden SQL for each operator, with values only in params.</li><li>Unknown identifiers and keys are rejected; limits hold; `_like` is prefix only.</li><li>Cursors round-trip; tampered, foreign and mismatched cursors are rejected.</li><li>The body digest binds; cross-use of the `api` claim is rejected.</li><li>Masks and row filters apply, and `filter_required` fires through select.</li><li>`next` is returned.</li></ul> |
| **G: glue** | `errors.py`, `startup_validation.py` (the storage rule and the `access` checks), `audit.py`, `metrics.py`, `tests/serving/test_storage_class_validation.py`, `tests/serving/test_postgres_validation_errors.py`, `tests/serving/live/test_postgres_live.py`, `tests/serving/live/postgres_tmp.py`, and the existing `test_typed_errors.py`, `test_startup_validation.py`, `test_audit_sink.py`, `test_ops_health_metrics.py` | <ul><li>All 6 codes are reachable over HTTP.</li><li>The storage rule refuses PII on `lakebase` (with a block or legacy), on `iceberg_direct`, and on undeclared `starrocks` and `cube`.</li><li>It allows PII on `postgres`, on any engine listed in `pii_object_storage_engines`, and allows a tokenized table on object storage.</li><li>Warnings fire for legacy deprecation, missing hardening and writable PII sources.</li><li>New audit fields pass C's allowlist; metric labels pass D's allowlist.</li><li>P0's pool metrics stay as deprecated duplicates.</li><li>The live suite skips itself unless `SEEKNAL_POSTGRES_LIVE` is set.</li></ul> |

**validation_commands**

```
PYTHONPATH=src python3 -B -m pytest -q -p no:cacheprovider tests/serving/test_postgres_sources_config.py tests/serving/test_postgres_session.py tests/serving/test_lakebase_endpoint.py tests/serving/test_discovery_postgres_introspect.py tests/serving/test_discovery_merge.py tests/serving/test_access_guardrails.py tests/serving/test_router_filter_rule.py tests/serving/test_select_compiler.py tests/serving/test_keyset_cursor.py tests/serving/test_select_http.py tests/serving/test_storage_class_validation.py tests/serving/test_postgres_validation_errors.py
PYTHONPATH=src python3 -B -m pytest -q -p no:cacheprovider tests/serving --ignore=tests/serving/test_serving_image.py
```

**Test strategy**

- **Unit tests in the sandbox** use `fake_postgres_catalog.py`, a DB-API fake. It records `(sql, params)`, scripts `pg_catalog`, `SHOW` and `_lakebase_sync_state` rows, captures connect kwargs, and can script sequences of refused connections for the cold-start tests. Time comes from an injected clock, so there are no real sleeps and no sockets.
- **The live suite** is run by the local agent outside the sandbox:

  `SEEKNAL_POSTGRES_LIVE=1 PYTHONPATH=src python3 -B -m pytest -q -p no:cacheprovider tests/serving/live/test_postgres_live.py`

  - **Setup:** `postgres_tmp.py` runs `initdb` and `pg_ctl` in a temp directory, on a Unix socket with a random port. Alternatively, `SEEKNAL_POSTGRES_LIVE_DSN` points at a throwaway `docker run postgres:16`.
  - **Fixtures:** the suite applies the §8 SQL with generated passwords, seeds 1M synthetic rows and writes sync-state rows.
  - **Assertions:**
    - introspection ignores partial, expression and invalid indexes;
    - a read-only source rejects INSERT with SQLSTATE 25006;
    - the `SHOW` values match;
    - a timeout produces `engine_timeout`;
    - a filtered query passes and an unfiltered one gets `filter_required`;
    - keyset pages have no gaps;
    - masks render as `'****'`;
    - `EXPLAIN` shows an Index Scan;
    - sync-state lag is reported;
    - `pii_cross_engine` fires.
  - **Cold start** is simulated with `pg_ctl stop`:
    - a delayed `pg_ctl start` during a query, which the bounded retry survives;
    - a long stop, which produces `engine_starting`.
  - **Role guard:** connecting as a superuser, or as a `BYPASSRLS` role, is refused.
  - **Optional, later:** a run against the atlas-lakebase GCP pilot (synthetic only, ADR-0007). It would cover a suspended endpoint with the wake API, plus the frozen-snapshot behaviour of a read-only endpoint. It is not part of this run.

**Private oracle tests** (written by the local agent, not given to workers)

- **Select API input:**
  - hostile identifiers return `select_invalid` before any AST exists;
  - hostile values appear only in params;
  - a masked column in a select's `where` or `order_by` returns `access_denied`;
  - a cursor with a flipped bit, from another table, or with another ordering returns `cursor_invalid`;
  - one changed body byte returns `grant_invalid`, and a select grant is refused on `/v1/query` and the reverse.
- **Guardrails:**
  - each of these returns `filter_required`: `CAST(id AS text)='1'`, `id=1 OR true`, `1=1 AND id IS NOT NULL`, a query filtered only by row filters, and two levels of nesting;
  - `OFFSET 100000` returns `offset_too_large`;
  - an unindexed ORDER BY returns `sort_not_indexed`.
- **Placement and PII:**
  - `pg_report`↔`lb_ops` and `pg_report`↔`lake` joins return `query_spans_engines`;
  - PII with a hint returns `pii_cross_engine`;
  - **the CBN YAML refuses startup with the §2.3 message when `src_pii` is moved to `lb_ops`, or when it is placed statically on `lb_ops` or `lake`;**
  - legacy `lakebase` hosting PII refuses until it is relabelled `postgres` or declared `storage_class: local`;
  - discovery never exposes an unlisted schema, or a revoked table or column.
- **Secrets:** a planted DSN password never appears in a response, log, audit record, metric or `/readyz`.
- **Compatibility and writes:**
  - a legacy single-`dsn_env` `lakebase` config routes and writes exactly as before;
  - a write to a table that exists only on read-only sources is refused before any connection is opened.
- **Cold start:**
  - 20 concurrent requests to a refusing endpoint produce one wake call and one probe sequence, and none waits past its own deadline.
  - The control-plane token never appears in logs, errors or metrics.
- **Lakebase safety:**
  - a DSN for an API-created (`neon_superuser`) role is refused at pool start;
  - `sslmode=require` is refused on `lakebase`.

## 12. Risks

1. **Superset SQL shapes may trip the guardrails.** Mitigations:
   - small tables default to `scan`;
   - the virtual-dataset rule;
   - a `warn`-mode rollout with the rejection metric;
   - running all 20 report SQLs through `/v1/explain` before go-live.
2. **Stale `reltuples`.** The pipeline runs `ANALYZE` after each load, and overrides can pin a table's pattern.
3. **Freshness depends on the pipeline** upserting `_lakebase_sync_state` after each load, including into the `postgres` report DB. Make it a pipeline acceptance item.
4. **Poolers that drop startup options.** Covered by `SHOW` verification, backed by `ALTER ROLE … SET`.
5. **P0 drift.** Start only after P0 is imported and verified. Before sealing, re-read `errors.py`, `startup_validation.py`, `audit.py` and `metrics.py`, including the pool metric names.
6. **Issuer support for `select.v1`** is outside this run, which is why `/v1/select` is third in the cut order.
7. **Connection budget.** `instances × Σ pool_max` must stay under the role's `CONNECTION LIMIT`. E warns but cannot prove it.
8. **Misdeclared storage.** The rule trusts the declared `storage_class` on `postgres`, `starrocks` and `cube`, so a `postgres` source on S3-backed storage would pass as `local`. Mitigations: config review of that declaration, and fail-closed defaults where the class is ambiguous (legacy `lakebase`, `starrocks`, `cube`).
9. **Lakebase cold start.** There is no wake-on-connect (ADR-0005), and restarts happen even with `-1`.
   - Mitigations: `suspend_timeout_seconds: -1`, plus the single-flight wake API in S2. If S2 is cut, `wake: none` serves nothing until the control plane resumes the endpoint.
   - Project-token issuance has not been exercised yet (`lbc:` §7).
10. **Lakebase maturity.** The following are not ready, so the `lakebase` kind ships for the GCP pilot and future use, not for CBN on-prem next week (`lbc:` §7):
    - read-only endpoints serve frozen snapshots;
    - `sync-table` has never run live, and breaks when views depend on the target;
    - there is no on-prem install, no monitoring, and no second pageserver.

    Serving does not rely on read-only endpoints for fresh reads, on wake-on-connect, or on a freshness API.
11. **safeData timing.** A tokens-only lake next week would be unsafe. Missing: Serving reveal, a chosen reveal model, lookup tokenize, erasure and the review. Keep the `postgres` replica and the keyed HMAC until R2's gate is met.
