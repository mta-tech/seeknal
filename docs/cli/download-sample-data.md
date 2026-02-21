---
summary: Download sample e-commerce datasets for tutorials and testing
read_when: You want to quickly get started with sample data to learn Seeknal
related:
  - init
  - draft
  - run
---

# seeknal download-sample-data

Download sample e-commerce datasets designed for learning Seeknal and testing
pipelines. These datasets provide a realistic e-commerce data model.

## Synopsis

```bash
seeknal download-sample-data [OPTIONS]
```

## Description

The `download-sample-data` command downloads four normalized CSV files that
demonstrate a realistic e-commerce data model:

- **customers.csv**: Customer information (20 customers)
- **products.csv**: Product catalog (20 products)
- **orders.csv**: Order headers (40 orders)
- **sales.csv**: Order line items (100 sales)

These datasets are designed for learning Seeknal pipeline building, testing
ELT workflows, and demonstrating feature store concepts.

## Options

| Option | Description |
|--------|-------------|
| `--output-dir`, `-o` | Output directory for sample data files (default: `data/sample/`) |
| `--force`, `-f` | Overwrite existing sample data files |

## Examples

### Download to default location

```bash
seeknal download-sample-data
```

### Download to custom directory

```bash
seeknal download-sample-data --output-dir ./my_data
```

### Overwrite existing files

```bash
seeknal download-sample-data --force
```

## Dataset Schema

### customers.csv
| Column | Description |
|--------|-------------|
| customer_id | Unique customer identifier |
| name | Customer name |
| email | Email address |
| region | Geographic region |

### products.csv
| Column | Description |
|--------|-------------|
| product_id | Unique product identifier |
| name | Product name |
| category | Product category |
| price | Unit price |

### orders.csv
| Column | Description |
|--------|-------------|
| order_id | Unique order identifier |
| customer_id | Foreign key to customers |
| order_date | Date of order |
| status | Order status |

### sales.csv
| Column | Description |
|--------|-------------|
| sale_id | Unique sale identifier |
| order_id | Foreign key to orders |
| product_id | Foreign key to products |
| quantity | Quantity ordered |

## See Also

- [seeknal init](init.md) - Initialize a new project
- [seeknal draft](draft.md) - Generate template files
- [seeknal run](run.md) - Execute pipeline
