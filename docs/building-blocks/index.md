# Building Blocks

Seeknal provides modular building blocks that you can combine to create powerful data pipelines. This section describes each building block in detail.

---

## What Are Building Blocks?

Building blocks are the fundamental components of Seeknal pipelines:

- **Sources**: Define where your data comes from
- **Transforms**: Specify how to transform your data
- **Common Config**: Reusable column mappings, rules, and SQL snippets
- **Aggregations**: Aggregate data across entities and time
- **Feature Groups**: Organize features for ML use cases
- **Semantic Models**: Define metrics for analytics
- **Tasks**: Advanced processing with Python

---

## When to Use Building Blocks

### Use Building Blocks When:
- You need detailed reference information
- You're designing complex pipelines
- You want to understand available options
- You're troubleshooting pipeline issues

### Use Tutorials When:
- You're learning Seeknal for the first time
- You want hands-on examples
- You prefer step-by-step guidance

---

## Building Blocks Overview

| Block | Description | Use Case |
|-------|-------------|----------|
| [Sources](sources.md) | Data ingestion from files, databases, APIs | Getting data into Seeknal |
| [Transforms](transforms.md) | Data transformations with SQL or Python | Cleaning, enriching, reshaping data |
| [Common Config](common-config.md) | Shared column mappings, rules, SQL snippets | DRY pipelines, consistent naming |
| [Aggregations](aggregations.md) | First and second-order aggregations | Computing metrics, rollups |
| [Feature Groups](feature-groups.md) | Feature organization with point-in-time joins | ML feature engineering |
| [Semantic Models](semantic-models.md) | Metric definitions for analytics | Business intelligence, KPIs |
| [Tasks](tasks.md) | Advanced Python processing | Custom logic, complex operations |

---

## Related Concepts

- [Pipeline Builder Workflow](../concepts/pipeline-builder.md) - How building blocks fit into the workflow
- [YAML vs Python](../concepts/yaml-vs-python.md) - Choosing your implementation approach
- [Virtual Environments](../concepts/virtual-environments.md) - Safe development with building blocks

---

**Next**: Learn about [Sources](sources.md) or return to [Choosing Your Path](../getting-started/)
