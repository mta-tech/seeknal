# Feature serving end-to-end

This example shows the complete ownership boundary:

1. Seeknal creates a Feature Group.
2. `seeknal run` materializes its applied revision into
   `feature_online.customer_profile__live`.
3. A contract-only Feature Service selects fields from that Feature Group.
4. `seeknal atlas feature-service plan|compile|publish` reads the applied
   manifest and run state.
5. Atlas stores the immutable contract and opens the guided activation page.
6. The service owner reviews the request, governance approves it, and the
   activation worker provisions and verifies exact OpenFGA tuples.
7. Atlas enables the online playground only when the Seeknal publication
   ledger, read-only runtime, and verified policy binding all agree.

Data Catalog never materializes data. If the Seeknal run has not produced
successful `atlas_online` evidence for the exact applied fingerprint, compile
and publish fail closed.

## Database profile

Copy `profiles.yml.example` to the YAML or Python project as `profiles.yml`.
Set the writer credentials provisioned by Atlas:

```bash
export ATLAS_PG_HOST=localhost
export ATLAS_PG_PORT=5432
export ATLAS_PG_DATABASE=seeknal
export ATLAS_FEATURE_MATERIALIZER_PASSWORD=replace-me
```

The login must be `seeknal_feature_materializer`. It assumes the
`feature_online_writer` role during the atomic staging-to-live swap. Do not use
the read-only `seeknal_feature_server` credentials.

## YAML authoring

```bash
cd examples/feature-serving-e2e/yaml
cp ../profiles.yml.example profiles.yml
seeknal parse
seeknal run --profile profiles.yml --full
seeknal atlas feature-service plan feature_service.customer_analytics
seeknal atlas feature-service compile feature_service.customer_analytics \
  --output target/customer-analytics.contract.json
seeknal atlas feature-service publish feature_service.customer_analytics \
  --request-activation \
  --consumer recommendation-api \
  --consumer-kind application \
  --capability consume_online
```

The Feature Group is explicitly created in
`seeknal/feature_groups/customer_profile.yml`; the Feature Service is declared
separately in `seeknal/feature_services/customer_analytics.yml`.

This minimal flow publishes the applied default state and requests activation
for the `development` environment. To publish an isolated environment instead,
create and apply it first (`seeknal env plan <name>` followed by
`seeknal run --env <name>`), then pass the same name to
`seeknal atlas feature-service publish --environment <name>`.

## Python authoring

The equivalent Python resources are under `python/seeknal`. Run the same
commands from the `python` directory. Discovery normalizes the Python decorators
to the same DAG and publication contract as YAML.

## Expected online objects

After a successful run:

- `feature_online.customer_profile__live` contains feature values plus
  provenance columns.
- `feature_online._online_publications` records the current revision, schema
  hash, run ID, row count, entity keys, and publication time.
- `seeknal_feature_server` can read both objects but cannot insert, update,
  delete, create, or swap serving tables.

Publication is idempotent for the same service/version/variant and immutable
contract. `--request-activation` creates an approval request; it never grants
access. Data Catalog derives the exact service and Feature View objects from
the contract, and records a policy binding only after OpenFGA write, read-back,
and permission checks succeed.
