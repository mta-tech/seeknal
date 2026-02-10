# Parallel Execution

Seeknal automatically executes independent pipeline tasks in parallel for faster performance.

---

## Overview

When pipeline tasks don't depend on each other, Seeknal runs them concurrently. This parallelization happens automatically—you don't need to configure it.

---

## How It Works

### Dependency Analysis

Seeknal analyzes your pipeline to identify dependencies:

```yaml
# These can run in parallel (no dependencies)
sources:
  - name: sales
  - name: customers
  - name: products

# This waits for sources
transforms:
  - name: enriched_sales
    depends_on: [sales, customers, products]
```

**Execution**:
1. `sales`, `customers`, `products` run in parallel
2. `enriched_sales` runs after all three complete

### Automatic Parallelization

```yaml
# All independent tasks run in parallel
transforms:
  - name: clean_sales
    depends_on: [raw_sales]

  - name: clean_customers
    depends_on: [raw_customers]

  - name: clean_products
    depends_on: [raw_products]

# This waits for all transforms
aggregations:
  - name: daily_metrics
    depends_on: [clean_sales, clean_customers, clean_products]
```

**Execution**:
1. `clean_sales`, `clean_customers`, `clean_products` run in parallel
2. `daily_metrics` runs after all three complete

---

## Performance Benefits

### Sequential Execution

```
Task 1: 5 minutes
Task 2: 5 minutes
Task 3: 5 minutes
Total: 15 minutes
```

### Parallel Execution

```
Task 1: 5 minutes ┐
Task 2: 5 minutes ├─→ 5 minutes (all run together)
Task 3: 5 minutes ┘
Total: 5 minutes
```

**Speedup**: 3x faster for three independent tasks

---

## Configuring Parallelism

### Default Behavior

Seeknal automatically detects and parallelizes independent tasks.

### Manual Control

For advanced use cases, you can control parallelism:

```yaml
# Limit concurrent tasks
execution:
  max_parallel: 4
```

---

## Best Practices

1. **Design for independence** when possible
2. **Let Seeknal handle** parallelization automatically
3. **Monitor performance** for bottlenecks
4. **Use environments** to test parallel execution

---

## Troubleshooting

### Tasks Not Running in Parallel

**Issue**: Tasks that should run in parallel are running sequentially.

**Check**:
- Are there hidden dependencies?
- Is there a resource limitation?
- Are tasks sharing the same output?

### Performance Issues

**Issue**: Parallel execution is slower than expected.

**Solutions**:
- Reduce `max_parallel` if resource-constrained
- Check for I/O bottlenecks
- Profile task execution times

---

## Related Topics

- [Pipeline Builder Workflow](pipeline-builder.md) - Core workflow
- [Change Categorization](change-categorization.md) - Understanding dependencies
- [Building Blocks: Tasks](../building-blocks/tasks.md) - Task configuration

---

**Return to**: [Concepts Overview](index.md)
