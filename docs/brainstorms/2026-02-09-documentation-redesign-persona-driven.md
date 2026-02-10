# Documentation Redesign: Persona-Driven Pipeline Builder

**Date**: 2026-02-09
**Status**: Brainstorm
**Related**: None

---

## What We're Building

A complete from-scratch documentation redesign for Seeknal that introduces the **pipeline builder workflow** (init → draft → apply → run --env) through **three persona-driven learning paths**, followed by unified concepts and advanced building blocks.

### Target Personas

1. **Data Engineer**: ELT pipelines, data orchestration, incremental models
2. **Analytics Engineer**: Semantic modeling, business metrics, self-serve analytics
3. **ML Engineer**: Feature stores, point-in-time joins, training/serving parity

### Key Requirements

- **5-minute Quick Start** - Practical first, see value immediately
- **Equal YAML/Python prominence** - Show both side-by-side from the start
- **Linear progression with real use cases** - Basic → intermediate → advanced
- **Semantic layer as advanced topic** - Master pipelines first, then add semantics
- **YAML workflow first** - Building blocks introduced through YAML, Python internals later

---

## Proposed Documentation Structure

```
docs/
├── index.md                              # NEW: Homepage with navigation cards
│
├── quick-start/                          # NEW: 5-minute getting started
│   └── index.md                          # Install → hello world → first pipeline
│
├── getting-started/                      # NEW: Three persona-driven paths
│   ├── index.md                          # Choose your path overview
│   │
│   ├── data-engineer-path/               # NEW: Data Engineer journey
│   │   ├── index.md                      # Path overview
│   │   ├── 1-elt-pipeline.md             # Build ELT pipeline (API → warehouse)
│   │   ├── 2-incremental-models.md       # Add incremental processing
│   │   └── 3-production-environments.md  # Virtual environments + promotion
│   │
│   ├── analytics-engineer-path/          # NEW: Analytics Engineer journey
│   │   ├── index.md                      # Path overview
│   │   ├── 1-semantic-models.md          # Define semantic models
│   │   ├── 2-business-metrics.md         # Create metrics (KPIs, ratios, cumulative)
│   │   └── 3-self-serve-analytics.md     # Deploy for BI consumption
│   │
│   └── ml-engineer-path/                 # NEW: ML Engineer journey
│       ├── index.md                      # Path overview
│       ├── 1-feature-store.md            # Build feature groups with point-in-time joins
│       ├── 2-second-order-aggregation.md # Multi-level feature engineering
│       └── 3-training-serving-parity.md  # Online serving + model integration
│
├── concepts/                              # EXISTING: Unified foundational concepts
│   ├── index.md                          # NEW: Concepts overview
│   ├── pipeline-builder.md               # NEW: The init → draft → apply → run workflow
│   ├── yaml-vs-python.md                 # EXISTING: When to use each
│   ├── virtual-environments.md           # EXISTING: Isolated development
│   ├── change-categorization.md          # EXISTING: Breaking/non-breaking changes
│   ├── parallel-execution.md             # EXISTING: Performance optimization
│   └── glossary.md                       # EXISTING: Terminology
│
├── building-blocks/                      # NEW: Advanced building blocks
│   ├── index.md                          # Overview of all building blocks
│   ├── sources.md                        # Data sources (files, databases, APIs)
│   ├── transforms.md                     # Data transformations
│   ├── aggregations.md                   # First and second-order aggregations
│   ├── feature-groups.md                 # Feature store concepts
│   ├── semantic-models.md                # Semantic layer concepts
│   └── tasks.md                          # DuckDBTask and SparkEngine internals
│
├── semantic-layer/                       # NEW: Dedicated semantic layer section
│   ├── index.md                          # Semantic layer overview
│   ├── semantic-models.md                # Deep dive on semantic models
│   ├── metrics.md                        # Deep dive on metrics
│   ├── deployment.md                     # StarRocks deployment
│   └── querying.md                       # Query syntax and patterns
│
├── reference/                            # EXISTING: Technical reference
│   ├── cli.md                            # NEW: Unified CLI reference
│   ├── yaml-schema.md                    # EXISTING: YAML reference
│   └── configuration.md                  # EXISTING: Config options
│
├── api/                                  # EXISTING: API documentation
└── guides/                               # EXISTING: Additional guides
```

---

## Key Design Decisions

### Decision 1: Three-Path Structure

**Approach**: Create three separate getting-started paths, one for each persona.

**Rationale**:
- Different personas have different goals and terminology
- Allows each persona to see relevant examples immediately
- Reduces cognitive load by not showing irrelevant concepts
- Enables linear progression within each path

**Tradeoffs**:
- More documentation to maintain
- Risk of content duplication
- Users may not know which path to choose

**Mitigation**:
- Clear "Choose Your Path" overview with use case descriptions
- Cross-reference between paths where concepts overlap
- Extract common concepts to unified `concepts/` section

---

### Decision 2: Quick Start → Persona Paths → Unified Concepts

**Approach**: Three-stage progression: Quick Start (5 min) → Persona Path (1-2 hours) → Unified Concepts (reference).

**Rationale**:
- **Quick Start**: Gets users running immediately, builds confidence
- **Persona Paths**: Teaches through real use cases, learn concepts in context
- **Unified Concepts**: Provides mental model and reference for deeper understanding

**Tradeoffs**:
- Some repetition between Quick Start and persona paths
- Users may skip concepts and miss important foundations

**Mitigation**:
- Quick Start focuses on "just works" without deep explanation
- Each persona path references relevant concept docs for deeper dives
- Concepts section written as reference, not tutorial

---

### Decision 3: YAML and Python Side-by-Side

**Approach**: Show YAML and Python examples side-by-side from the beginning, with equal prominence.

**Rationale**:
- Different personas prefer different interfaces
- Python better for complex logic and programmatic control
- YAML better for declarative simplicity and version control
- Seeknal supports both equally - docs should reflect this

**Tradeoffs**:
- Increases documentation length
- May confuse beginners who don't know which to choose
- Examples become more complex

**Mitigation**:
- Clear guidance on "When to use YAML vs Python" in concepts
- Each persona path emphasizes the format most relevant to that role
- Side-by-side format uses tabs or columns for easy comparison

---

### Decision 4: Semantic Layer as Advanced Topic

**Approach**: Treat semantic layer as an advanced topic, not integrated into basic pipeline builder workflow.

**Rationale**:
- Semantic layer builds on pipeline builder concepts
- Analytics Engineer persona path focuses heavily on it
- Other personas (DE, MLE) may not need it immediately
- Reduces initial cognitive load

**Tradeoffs**:
- Analytics Engineers must wait to learn their primary use case
- May silo semantic layer as "Analytics only"

**Mitigation**:
- Analytics Engineer path can be completed independently
- Quick Start mentions semantic layer as a capability
- Cross-references from other paths to semantic layer when relevant

---

### Decision 5: Building Blocks After Persona Paths

**Approach**: Introduce building blocks (DuckDBTask, SparkEngine, etc.) after users complete a persona path.

**Rationale**:
- Users learn by doing first (YAML workflow)
- Understand "what it does" before "how it works"
- Building blocks become reference for advanced customization

**Tradeoffs**:
- Advanced users may want to understand internals earlier
- Hides power user features from initial discovery

**Mitigation**:
- Building blocks section is well-organized for lookup
- Concepts section explains the architecture at a high level
- Clear links from persona paths to relevant building blocks

---

## Persona Path Details

### Data Engineer Path: ELT Pipeline

**Chapter 1: Build an ELT Pipeline**
- Ingest data from REST API
- Clean and transform with DuckDB
- Output to data warehouse
- Shows: `seeknal init`, `seeknal draft source`, `seeknal apply`, `seeknal run`

**Chapter 2: Add Incremental Processing**
- Configure incremental models
- Handle change data capture
- Schedule automated runs
- Shows: Change categorization, incremental execution

**Chapter 3: Production Environments**
- Create virtual environment
- Test in isolation
- Promote to production
- Shows: `seeknal env plan`, `seeknal env apply`, `seeknal env promote`

**Real Use Case**: E-commerce order data pipeline

---

### Analytics Engineer Path: Semantic Modeling

**Chapter 1: Define Semantic Models**
- Create semantic model from raw data
- Define entities, dimensions, measures
- Query semantic model
- Shows: Semantic model YAML, `seeknal query`

**Chapter 2: Create Business Metrics**
- Simple metrics (KPIs)
- Ratio metrics (conversion rate)
- Cumulative metrics (running totals)
- Shows: Metric definitions, derived metrics

**Chapter 3: Deploy for Self-Serve Analytics**
- Deploy metrics to StarRocks
- Create materialized views
- Connect BI tools
- Shows: `seeknal deploy-metrics`, StarRocks integration

**Real Use Case**: E-commerce business metrics (revenue, AOV, conversion)

---

### ML Engineer Path: Full ML Pipeline

**Chapter 1: Build Feature Store**
- Create feature groups
- Implement point-in-time joins
- Generate training data
- Shows: FeatureGroup, HistoricalFeatures, `event_time_col`

**Chapter 2: Second-Order Aggregation**
- Build multi-level features
- User → region aggregations
- Window and ratio features
- Shows: `@second_order_aggregation`, aggregation types

**Chapter 3: Training-to-Serving Parity**
- Serve features online
- Maintain consistency between training and serving
- Integrate with model inference
- Shows: OnlineFeatures, model deployment

**Real Use Case**: Customer churn prediction with temporal features

---

## Content Reuse Strategy

### What to Keep (Reuse Existing Content)

- **Concept docs**: Excellent explanations already exist
- **Second-order aggregations**: Comprehensive with good examples
- **Point-in-time joins**: Best documentation in codebase
- **Semantic layer guide**: Solid foundation, needs persona examples
- **YAML schema reference**: Accurate and complete

### What to Create (New Content)

- **Quick Start**: 5-minute hello world
- **Persona paths**: Three complete learning journeys
- **Pipeline builder concept**: Unified workflow explanation
- **Building blocks**: Task internals (DuckDBTask, SparkEngine)
- **CLI reference**: Single page documenting all commands
- **Homepage**: Navigation cards for MkDocs

### What to Refactor (Reorganize Existing)

- **Getting Started Guide**: Split into persona paths
- **Concepts**: Reorganize with pipeline builder first
- **Guides**: Some become persona path content, some stay as guides

---

## Success Criteria

**For Documentation Quality**:
1. Time-to-first-value: User runs first pipeline in <5 minutes
2. Persona relevance: Each user finds their path without confusion
3. Conceptual understanding: Users can explain the pipeline builder workflow
4. Retention: Users return to docs as reference, not just tutorial

**For User Experience**:
1. Clear entry point: Homepage with obvious next step
2. No dead ends: Every page links to relevant next steps
3. Equal formats: YAML and Python both accessible
4. Progressive disclosure: Simple first, complex when needed

**For Maintainability**:
1. DRY: Minimize duplication through cross-references
2. Modular: Each section can be updated independently
3. Searchable: Clear organization and navigation
4. Versioned: Docs match released software versions

---

## Open Questions

1. **Should we include video walkthroughs for each persona path?**
   - Pro: Different learning styles, more engaging
   - Con: Production overhead, version drift

2. **How do we handle the transition from persona paths to building blocks?**
   - Need clear guidance on "what's next"
   - Consider "Advanced Topics" section per persona

---

## Answered Questions (Refined Decisions)

### Q1: Should persona paths be independent or ordered?
**Answer**: Quick Start is the only required prerequisite. After Quick Start, users can choose any persona path independently.

**Rationale**:
- Quick Start teaches the universal pipeline builder workflow (init → draft → apply → run)
- Each persona path reinforces this workflow while teaching domain-specific concepts
- Paths reference the Quick Start for foundational workflow concepts
- Cross-references between paths for related topics

### Q2: How to handle multi-persona users?
**Answer**: Cross-references only - each path has "See also" links to related topics in other paths.

**Rationale**:
- Keeps paths independent and maintainable
- Users can dip into other paths as needed
- No complex "mix and match" documentation to maintain
- Natural progression: complete one path, explore others as interests grow

### Q3: Should Quick Start be persona-agnostic or specific?
**Answer**: Persona-agnostic - use a generic pipeline example (load CSV → transform → save).

**Rationale**:
- Inclusive - all personas can relate to basic data transformation
- Quick Start focuses on the workflow mechanics, not domain specifics
- Generic example doesn't alienate any persona
- Each persona path then shows domain-specific applications of the same workflow

---

## Quick Start Design

### The 10-Minute Quick Start

**Goal**: Get Seeknal installed, create a project, and run a pipeline end-to-end with enough explanation to understand what's happening.

**Generic Example**: Sales data pipeline
- Source: CSV file with sales transactions
- Transform: Calculate daily revenue by product
- Output: Save results to Parquet

**Structure** (10 minutes total):

1. **Install & Setup** (2 minutes)
   - Installation command
   - Verify installation
   - Initialize project

2. **Understand the Pipeline Builder Workflow** (2 minutes)
   - Brief explanation of: init → draft → apply → run
   - Visual diagram of the workflow
   - Why this workflow matters (safety, versioning, collaboration)

3. **Create Your First Pipeline** (4 minutes)
   - Use `seeknal draft` to generate templates
   - Review generated YAML with inline explanations
   - Make a simple edit (add a filter)
   - Apply each draft

4. **Run and See Results** (2 minutes)
   - Execute with `seeknal run`
   - View output
   - Next steps guidance

**What Users Learn**:
- How Seeknal projects are structured
- The draft → apply workflow and WHY it exists
- Basic pipeline concepts: source, transform, output
- How to run a pipeline
- Where to find more information

**What Users Don't Learn** (intentionally):
- Feature groups, semantic models, advanced aggregations
- Virtual environments, change categorization
- DuckDB vs Spark engine details
- Python API (YAML only)

**Next Step**: "Choose your path to continue learning: Data Engineer | Analytics Engineer | ML Engineer"

---

## Homepage Design (MkDocs Navigation)

### index.md Navigation Cards

```
┌─────────────────────────────────────────────────────────────────┐
│                     Welcome to Seeknal                          │
│   All-in-one platform for data and AI/ML engineering            │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│   📦 Install  │  🚅 Quick    │  👷 Choose   │   📖 Concepts │  📚 Reference │
│              │   Start      │   Your Path  │              │              │
│              │              │              │              │              │
│ Get Seeknal  │ Run your     │ Data Eng │  │ Pipeline     │ CLI commands │
│ running in   │ first        │ Analytics│  │ Builder      │ YAML schema  │
│ minutes      │ pipeline in  │ ML Eng   │  │ YAML vs Py   │ Config       │
│              │ 10 minutes   │           │  │ Virt. Env    │ API docs     │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘
```

**Card Details**:

1. **📦 Install**
   - Prerequisites (Python 3.11+)
   - Install via pip
   - Verify installation
   - Quick troubleshooting

2. **🚅 Quick Start** (prominent - recommended first step)
   - 10-minute tutorial
   - Generic pipeline example
   - Learn the workflow
   - Links to: Choose Your Path

3. **👷 Choose Your Path** (the main navigation decision)
   - Three cards side-by-side:
     - **Data Engineer**: ELT pipelines, incremental processing, environments
     - **Analytics Engineer**: Semantic models, business metrics, BI deployment
     - **ML Engineer**: Feature stores, point-in-time joins, training/serving
   - Each card links to its respective path index

4. **📖 Concepts**
   - Foundational understanding
   - Pipeline builder workflow
   - YAML vs Python
   - Virtual environments
   - Change categorization
   - Reference material, not tutorial

5. **📚 Reference**
   - CLI command reference (NEW - unified page)
   - YAML schema reference
   - Configuration options
   - API documentation

**Footer**: "New to Seeknal? Start with [Quick Start](quick-start/)."

---

## Next Steps

1. **Validate this approach** with stakeholders and actual users from each persona
2. **Create homepage wireframe** with navigation cards
3. **Write Quick Start** (highest priority, highest impact)
4. **Develop Data Engineer path** first (most foundational)
5. **Extract and reorganize existing content** into new structure
6. **Build CLI reference page** (current gap)
7. **Add building blocks section** (lowest priority, advanced users only)

---

## Related Documents

- [Second-Order Aggregations Brainstorm](/docs/brainstorms/2026-01-30-second-order-aggregations-brainstorm.md)
- [SQLMesh-Inspired Features Brainstorm](/docs/brainstorms/2026-02-08-sqlmesh-inspired-features-brainstorm.md)
- [Unified Developer Experience Brainstorm](/docs/brainstorms/2026-02-09-unified-developer-experience-brainstorm.md)
- [MkDocs Web Documentation Brainstorm](/docs/brainstorms/2026-02-09-mkdocs-web-documentation-brainstorm.md)
