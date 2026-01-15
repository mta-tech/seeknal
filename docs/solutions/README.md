# Solutions Documentation

This directory contains documentation for problems solved during the development of Seeknal. Each solution document follows a structured format with YAML frontmatter for easy categorization and discovery.

## Structure

```
docs/solutions/
├── README.md
└── testing/
    └── test-isolation-fixes.md
```

## Document Format

Each solution document includes:

- **YAML Frontmatter**: Metadata for categorization and search
- **Problem Statement**: Clear description of the issue
- **Investigation**: Symptoms and root cause analysis
- **Solution**: Complete working solution with code examples
- **Prevention Strategies**: Best practices to avoid recurrence
- **Related Documentation**: Cross-references to related docs

## Categories

- **Testing**: Test infrastructure, pytest fixtures, test isolation
- **Performance**: Optimization, benchmarking, resource management
- **Security**: SQL injection prevention, path validation, access control
- **Migration**: Upgrading dependencies, refactoring patterns

## Adding New Solutions

When documenting a solved problem:

1. Create a new markdown file in the appropriate category directory
2. Include YAML frontmatter with required fields:
   ```yaml
   ---
   title: Descriptive title
   category: Testing|Performance|Security|Migration
   component: Affected component (e.g., conftest.py, duckdbengine)
   symptom: User-visible symptoms
   root_cause: Technical root cause
   tags: [tag1, tag2, tag3]
   ---
   ```

3. Follow the document structure (problem → investigation → solution → prevention)
4. Include working code examples with syntax highlighting
5. Add cross-references to related documentation

## Recent Solutions

### [Test Isolation Fixes](testing/test-isolation-fixes.md)
**Category**: Testing | **Component**: conftest.py

Fixed test state pollution issues in PySpark and FeatureGroup tests by implementing comprehensive cleanup fixtures with proper security and error handling.

**Impact**: Fixed flaky tests, improved CI/CD reliability

## Search

To find solutions by tag or category:

```bash
# Find all testing-related solutions
grep -r "category: Testing" docs/solutions/

# Find solutions by tag
grep -r "pyspark" docs/solutions/

# Search by symptom
grep -r "flaky tests" docs/solutions/
```
