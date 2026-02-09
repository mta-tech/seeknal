# What's Next?

> **Congratulations!** :rocket: You've completed the Seeknal Quick Start.

You've learned the pipeline builder workflow and run your first pipeline. Now what?

---

## Choose Your Learning Path

Seeknal supports three main personas. Choose the one that matches your role:

### Data Engineer Path

**Build production data pipelines at scale.**

- Chapter 1: Build an ELT Pipeline — Ingest data from APIs, transform with DuckDB, output to warehouses
- Chapter 2: Add Incremental Models — Handle change data capture and schedule automated runs
- Chapter 3: Production Environments — Virtual environments, change categorization, and promotion

**Time commitment:** 3 chapters × 20-30 minutes

**Perfect for:** Data Engineers, ETL Developers, Pipeline Engineers

[Start Data Engineer Path →](../getting-started/data-engineer-path/)

---

### Analytics Engineer Path

**Create semantic models and business metrics for self-serve analytics.**

- Chapter 1: Define Semantic Models — Create entities, dimensions, and measures
- Chapter 2: Create Business Metrics — KPIs, ratios, cumulative metrics
- Chapter 3: Deploy for Self-Serve Analytics — StarRocks deployment and BI tool integration

**Time commitment:** 3 chapters × 20-30 minutes

**Perfect for:** Analytics Engineers, Data Analysts, BI Developers

[Start Analytics Engineer Path →](../getting-started/analytics-engineer-path/)

---

### ML Engineer Path

**Build feature stores with point-in-time joins for ML applications.**

- Chapter 1: Build Feature Store — Feature groups, point-in-time joins, training data
- Chapter 2: Second-Order Aggregation — Multi-level features and window functions
- Chapter 3: Training-to-Serving Parity — Online serving and model integration

**Time commitment:** 3 chapters × 20-30 minutes

**Perfect for:** ML Engineers, Data Scientists, MLOps Engineers

[Start ML Engineer Path →](../getting-started/ml-engineer-path/)

---

## Not Sure Which Path?

!!! question "Can't decide?"
    Here's a quick guide:

    - **Start with Data Engineer** if you want to understand the fundamentals
    - **Choose Analytics Engineer** if you care about business metrics and BI
    - **Choose ML Engineer** if you're building ML models and need features

    !!! tip "Pro tip"
        You can always come back and complete multiple paths! Each one reinforces the core concepts.

---

## Explore Concepts

Want to understand how Seeknal works under the hood?

- [Pipeline Builder Workflow](../concepts/pipeline-builder.md) — Deep dive on init → draft → apply → run
- [YAML vs Python](../concepts/yaml-vs-python.md) — When to use each approach
- [Virtual Environments](../concepts/virtual-environments.md) — Isolated development workflows
- [Change Categorization](../concepts/change-categorization.md) — Understanding breaking vs non-breaking changes
- [Glossary](../concepts/glossary.md) — All Seeknal terminology

---

## Reference Material

Looking up specific commands or syntax?

- [CLI Reference](../reference/cli.md) — All 40+ commands with examples
- [YAML Schema Reference](../reference/yaml-schema.md) — Complete schema documentation
- [Configuration Guide](../reference/configuration.md) — Project settings and profiles
- [Python API](../api/) — Full API documentation

---

## Community & Support

Join the Seeknal community:

- [GitHub Discussions](https://github.com/mta-tech/seeknal/discussions) — Ask questions, share ideas
- [Discord Server](https://discord.gg/seeknal) — Chat with other users
- [GitHub Issues](https://github.com/mta-tech/seeknal/issues) — Report bugs, request features

---

## Track Your Progress

Want to keep track of what you've learned?

- [ ] Quick Start (completed!)
- [ ] Data Engineer Path
- [ ] Analytics Engineer Path
- [ ] ML Engineer Path
- [ ] Concepts review
- [ ] Building Blocks deep dive

---

## Quick Reference

| Command | Purpose |
|---------|---------|
| `seeknal init <name>` | Create a new project |
| `seeknal draft <kind>` | Generate a template |
| `seeknal apply <file>` | Add to project |
| `seeknal run` | Execute pipeline |
| `seeknal status` | View applied resources |
| `seeknal --help` | All commands |

---

**You're ready to continue your Seeknal journey!** Choose your path above and let's build something great.
