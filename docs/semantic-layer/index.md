# Semantic Layer

The Seeknal Semantic Layer provides a unified, business-friendly view of your data for analytics and business intelligence.

---

## What is the Semantic Layer?

The semantic layer sits between your raw data and your analytics tools, providing:

- **Consistent Metrics**: Define business logic once, reuse everywhere
- **Self-Service Analytics**: Enable stakeholders to query data without SQL
- **Governance**: Control access and ensure data quality
- **Documentation**: Self-documenting data models

---

## Key Concepts

### Semantic Models

Semantic models define the structure of your data in business terms:

- **Entities**: Core objects (customers, products, orders)
- **Dimensions**: Attributes for filtering and grouping
- **Measures**: Quantitative values that can be aggregated

### Metrics

Metrics are calculations based on semantic models:

- **Simple Metrics**: Basic aggregations (SUM, COUNT, AVG)
- **Ratio Metrics**: Calculations between metrics (conversion rate)
- **Cumulative Metrics**: Running totals and YTD calculations
- **Derived Metrics**: Complex business logic

---

## Semantic Layer Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Analytics Tools                       │
│  Tableau | Power BI | Metabase | Custom Applications   │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                    Semantic Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │   Semantic   │  │    Metrics   │  │  Governed    │  │
│  │    Models    │→ │     API      │→ │    Access    │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────┐
│                    Data Warehouse                        │
│        StarRocks | PostgreSQL | DuckDB                  │
└─────────────────────────────────────────────────────────┘
```

---

## Getting Started

1. **[Semantic Models](semantic-models.md)** - Define your data model
2. **[Metrics](metrics.md)** - Create business metrics
3. **[Deployment](deployment.md)** - Deploy to production
4. **[Querying](querying.md)** - Query the semantic layer

---

## Use Cases

### Executive Dashboards
Provide consistent KPIs across all dashboards and reports.

### Self-Service Analytics
Enable business users to explore data without writing SQL.

### Data Products
Build reliable data products with governed metrics.

---

## Related Topics

- [Semantic Layer Guide](../guides/semantic-layer.md) - Comprehensive guide with CLI examples
- [YAML Schema Reference](../reference/yaml-schema.md) - Semantic model and metric YAML definitions
- [Testing & Audits](../guides/testing-and-audits.md) - Data quality validation

---

**Next**: Learn about [Semantic Models](semantic-models.md)
