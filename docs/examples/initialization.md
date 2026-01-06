# Initialization

This guide demonstrates how to set up and initialize Seeknal projects and entities. These are the foundational building blocks for all Seeknal workflows.

## Prerequisites

Before running these examples, ensure you have:

1. Seeknal installed (see [Installation Guide](../index.md#installation))
2. Configuration file set up with valid paths
3. Environment variables configured:
   ```bash
   export SEEKNAL_BASE_CONFIG_PATH="/path/to/config/directory"
   export SEEKNAL_USER_CONFIG_PATH="/path/to/config.toml"
   ```

## Project Setup

Projects are the top-level organizational unit in Seeknal. They group related entities, flows, and feature stores together.

### Creating a Project

```python
from seeknal.project import Project

# Create a new project with name and description
project = Project(
    name="my_feature_store",
    description="Feature store for customer analytics"
)

# Save to database (or load if it already exists)
project = project.get_or_create()

# The project now has an ID assigned
print(f"Project ID: {project.project_id}")
print(f"Project Name: {project.name}")
```

!!! note "Name Normalization"
    Project names are automatically converted to snake_case. For example,
    `"My Feature Store"` becomes `"my_feature_store"`.

### Listing All Projects

```python
from seeknal.project import Project

# Display all projects in a formatted table
Project.list()
```

Output:
```
| name             | description                        | created_at          | updated_at          |
|------------------|------------------------------------|---------------------|---------------------|
| my_feature_store | Feature store for customer analytics | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
```

### Updating a Project

```python
from seeknal.project import Project

# Load existing project
project = Project(name="my_feature_store").get_or_create()

# Update the description
project = project.update(description="Updated: Customer analytics feature store v2")
```

### Retrieving a Project by ID

```python
from seeknal.project import Project

# Get project by its unique identifier
project = Project.get_by_id(1)
print(f"Found project: {project.name}")
```

## Entity Setup

Entities represent domain objects in your feature store (e.g., users, products, transactions). They define the join keys used to associate features with specific instances.

### Creating an Entity

```python
from seeknal.entity import Entity

# Create a basic entity with a single join key
user_entity = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="Customer entity for retail features"
)
user_entity = user_entity.get_or_create()

print(f"Entity ID: {user_entity.entity_id}")
print(f"Join Keys: {user_entity.join_keys}")
```

### Entity with Multiple Join Keys

For composite keys, specify multiple columns in the `join_keys` list:

```python
from seeknal.entity import Entity

# Entity with composite key (multiple columns)
order_entity = Entity(
    name="order_item",
    join_keys=["order_id", "product_id"],
    description="Order item entity for transaction features"
)
order_entity = order_entity.get_or_create()
```

### Entity with PII Keys

Mark columns containing personally identifiable information for compliance tracking:

```python
from seeknal.entity import Entity

# Entity with PII keys for data privacy compliance
customer_entity = Entity(
    name="customer_profile",
    join_keys=["customer_id"],
    pii_keys=["email", "phone_number", "address"],
    description="Customer profile with PII tracking"
)
customer_entity = customer_entity.get_or_create()
```

!!! warning "PII Handling"
    PII keys are used for data privacy compliance and tracking. Ensure you
    follow your organization's data governance policies when handling PII data.

### Listing All Entities

```python
from seeknal.entity import Entity

# Display all registered entities
Entity.list()
```

Output:
```
| name             | join_keys              | pii_keys                    | description                    |
|------------------|------------------------|-----------------------------|--------------------------------|
| customer         | customer_id            | None                        | Customer entity for retail     |
| order_item       | order_id,product_id    | None                        | Order item entity              |
| customer_profile | customer_id            | email,phone_number,address  | Customer profile with PII      |
```

### Updating an Entity

```python
from seeknal.entity import Entity

# Load and update entity
entity = Entity(name="customer").get_or_create()
entity.update(
    description="Updated customer entity description",
    pii_keys=["email"]  # Add PII tracking
)
```

!!! note "Join Keys Are Immutable"
    Join keys cannot be modified after entity creation because they define
    the entity's identity. To change join keys, create a new entity.

### Setting Key Values for Point Lookups

When retrieving features for a specific entity instance, use `set_key_values()`:

```python
from seeknal.entity import Entity

# Load entity
entity = Entity(name="order_item", join_keys=["order_id", "product_id"]).get_or_create()

# Set specific values for lookup
entity = entity.set_key_values("ORD-12345", "PROD-6789")

# Access the key-value mapping
print(entity.key_values)
# Output: {'order_id': 'ORD-12345', 'product_id': 'PROD-6789'}
```

## Complete Initialization Example

Here's a complete example setting up a project with multiple entities:

```python
from seeknal.project import Project
from seeknal.entity import Entity

# Step 1: Create and initialize the project
project = Project(
    name="ecommerce_features",
    description="Feature store for e-commerce recommendation engine"
)
project = project.get_or_create()
print(f"Project initialized: {project.name} (ID: {project.project_id})")

# Step 2: Create customer entity
customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    pii_keys=["email"],
    description="Customer entity for user-based features"
)
customer = customer.get_or_create()
print(f"Customer entity created: {customer.name}")

# Step 3: Create product entity
product = Entity(
    name="product",
    join_keys=["product_id"],
    description="Product catalog entity"
)
product = product.get_or_create()
print(f"Product entity created: {product.name}")

# Step 4: Create interaction entity (composite key)
interaction = Entity(
    name="customer_product_interaction",
    join_keys=["customer_id", "product_id"],
    description="Customer-product interaction for collaborative filtering"
)
interaction = interaction.get_or_create()
print(f"Interaction entity created: {interaction.name}")

# View all entities
print("\nAll registered entities:")
Entity.list()
```

## Idempotent Operations

The `get_or_create()` pattern ensures idempotent operations:

```python
from seeknal.project import Project

# First call creates the project
project1 = Project(name="my_project").get_or_create()
print(f"Project ID: {project1.project_id}")

# Second call retrieves the existing project (no duplicate created)
project2 = Project(name="my_project").get_or_create()
print(f"Same Project ID: {project2.project_id}")

# Both variables reference the same project
assert project1.project_id == project2.project_id
```

This pattern is safe to run multiple times without side effects, making it ideal for deployment scripts and CI/CD pipelines.

## Error Handling

Handle common initialization errors gracefully:

```python
from seeknal.project import Project
from seeknal.entity import Entity
from seeknal.exceptions import ProjectNotFoundError, EntityNotFoundError, EntityNotSavedError

# Handle project not found
try:
    project = Project.get_by_id(9999)  # Non-existent ID
except ProjectNotFoundError as e:
    print(f"Project error: {e}")

# Handle entity not saved
try:
    entity = Entity(name="test_entity", join_keys=["id"])
    # Attempting to update without calling get_or_create() first
    entity.update(description="New description")
except EntityNotSavedError as e:
    print(f"Entity must be saved first: {e}")
```

See [Error Handling](error_handling.md) for more comprehensive exception handling patterns.

## Next Steps

After initializing your project and entities, you can:

1. **Create Flows** - Build data transformation pipelines ([Flows Example](flows.md))
2. **Set Up Feature Groups** - Store and manage features ([FeatureStore Example](featurestore.md))
3. **Configure Settings** - Customize Seeknal behavior ([Configuration Example](configuration.md))
