# Concepts Overview

Learn the mental model behind Seeknal and understand how its components work together.

---

## What Are Concepts?

Concepts are the fundamental ideas and patterns that underlie Seeknal's design. Understanding these concepts will help you use Seeknal effectively and troubleshoot issues.

---

## Core Concepts

### Pipeline Builder Workflow

The fundamental workflow for creating and deploying data pipelines:

```
init → draft → apply → run --env
```

[Learn more →](pipeline-builder.md)

### YAML vs Python

Seeknal supports two approaches for defining pipelines:

- **YAML**: Declarative, configuration-driven
- **Python**: Programmatic, flexible

[Learn more →](yaml-vs-python.md)

### Virtual Environments

Isolated workspaces for safe development and testing:

- Plan changes before applying
- Test in isolation
- Promote to production safely

[Learn more →](virtual-environments.md)

### Change Categorization

Understanding the impact of changes:

- **BREAKING**: Requires manual intervention
- **NON_BREAKING**: Safe to apply automatically
- **METADATA**: Documentation-only changes

[Learn more →](change-categorization.md)

### Parallel Execution

Run independent tasks concurrently for faster pipelines:

- Automatic parallelization
- Dependency management
- Performance optimization

[Learn more →](parallel-execution.md)

---

## Advanced Concepts

### Point-in-Time Joins

Prevent data leakage in ML features by joining data as it appeared at prediction time.

[Learn more →](point-in-time-joins.md)

### Second-Order Aggregations

Create hierarchical aggregations and multi-level rollups.

[Learn more →](second-order-aggregations.md)

---

## When to Read Concepts

- **Before starting**: Get oriented with key ideas
- **During learning**: Deepen your understanding
- **Troubleshooting**: Understand what's happening under the hood
- **Best practices**: Learn recommended patterns

---

## Learning Path

**New to Seeknal?** Start with these concepts in order:

1. [Pipeline Builder Workflow](pipeline-builder.md) - Understand the core workflow
2. [YAML vs Python](yaml-vs-python.md) - Choose your approach
3. [Virtual Environments](virtual-environments.md) - Safe development practices

**Experienced user?** Jump to advanced topics:

1. [Point-in-Time Joins](point-in-time-joins.md) - ML feature engineering
2. [Second-Order Aggregations](second-order-aggregations.md) - Advanced analytics
3. [Parallel Execution](parallel-execution.md) - Performance optimization

---

## Related Topics

- [Glossary](glossary.md) - Definitions of key terms
- [Building Blocks](../building-blocks/) - Detailed component reference
- [Getting Started](../getting-started/) - Hands-on tutorials

---

**Next**: Explore [Pipeline Builder Workflow](pipeline-builder.md) or return to [Choosing Your Path](../getting-started/)
