# OpenLineage schemas, for the tests

The OpenLineage events Lakeleto sends (`--openlineage`, `OPENLINEAGE_URL`) are validated against
these, exactly as published at openlineage.io (Apache-2.0, the OpenLineage project):

| File | Published at |
|---|---|
| `OpenLineage-2-0-2.json` | https://openlineage.io/spec/2-0-2/OpenLineage.json |
| `DataQualityAssertionsDatasetFacet-1-0-2.json` | https://openlineage.io/spec/facets/1-0-2/DataQualityAssertionsDatasetFacet.json |
| `DataQualityMetricsInputDatasetFacet-1-0-3.json` | https://openlineage.io/spec/facets/1-0-3/DataQualityMetricsInputDatasetFacet.json |
| `SymlinksDatasetFacet-1-0-1.json` | https://openlineage.io/spec/facets/1-0-1/SymlinksDatasetFacet.json |
| `CatalogDatasetFacet-1-0-0.json` | https://openlineage.io/spec/facets/1-0-0/CatalogDatasetFacet.json |

The first three are the same files as Covenant's (`tests/openlineage/` from its repository root),
which its `check --openlineage` events are held to. They are copies, so the tests need
no network. A new revision the events move to is a new copy here, and the constants in
`src/lineage.rs` name it.
