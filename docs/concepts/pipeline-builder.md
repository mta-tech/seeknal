# Pipeline Builder Workflow

The Pipeline Builder workflow is Seeknal's core pattern for creating, testing, and deploying data pipelines safely.

---

## What is the Pipeline Builder?

The Pipeline Builder is a workflow that ensures safe, collaborative data pipeline development:

```
init → draft → apply → run --env
```

Each step serves a specific purpose in the pipeline lifecycle.

---

## Workflow Steps

### 1. Initialize (`seeknal init`)

Create a new Seeknal project with the proper structure.

```bash
seeknal init --name my_project
```

**Creates**:
- Project directory structure
- Configuration files
- Database for metadata

### 2. Draft (`seeknal draft`)

Generate template files for sources, transforms, or other components.

```bash
seeknal draft source --name raw_sales
```

**Creates**:
- YAML template with inline comments
- Best practices built-in
- Ready for customization

### 3. Apply (`seeknal apply`)

Add your defined components to the project.

```bash
seeknal apply drafts/raw_sales.yaml
```

**Validates**:
- YAML syntax
- Dependencies
- Configuration

### 4. Run (`seeknal run`)

Execute the pipeline locally.

```bash
seeknal run
```

**Executes**:
- All components in dependency order
- Data transformations
- Output generation

### 5. Run with Environment (`seeknal run --env`)

Execute in isolated environments for testing.

```bash
seeknal run --env dev
seeknal run --env prod
```

**Enables**:
- Safe testing
- Production isolation
- Environment-specific configurations

---

## Why This Workflow?

### Safety

Review changes before they affect production:
- **Draft**: Generate and review templates
- **Apply**: Validate before adding to project
- **Run --env**: Test in isolation first

### Version Control

Track all changes with Git:
- YAML files are version controlled
- Review changes with pull requests
- Rollback when needed

### Collaboration

Work together effectively:
- Code reviews for pipelines
- Shareable templates
- Documented workflows

---

## Best Practices

1. **Always use environments** for production changes
2. **Review draft files** before applying
3. **Test in dev** before promoting to prod
4. **Commit frequently** to version control
5. **Document complex logic** in YAML comments

---

## Related Topics

- [Virtual Environments](virtual-environments.md) - Environment management
- [Change Categorization](change-categorization.md) - Understanding change impact
- [YAML vs Python](yaml-vs-python.md) - Choosing your approach

---

**Next**: Learn about [YAML vs Python](yaml-vs-python.md) or return to [Concepts Overview](index.md)
