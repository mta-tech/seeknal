# Spark Transformers Reference

This document catalogs all Scala transformers for porting to PySpark.

**Source:** `engines/spark-engine/src/main/scala/tech/mta/seeknal/transformers/`

**Total Transformers:** 30

**Last Updated:** 2026-01-14

---

## Transformer Catalog

### 1. FilterByExpr
**File:** `FilterByExpr.scala`

**Description:** Filter rows by SQL expression

**Scala Implementation:**
```scala
df.filter(expr(getExpression))
```

**PySpark Equivalent:**
```python
df.filter(expr(expression))
```

**Complexity:** Low

---

### 2. ColumnRenamed
**File:** `ColumnRenamed.scala`

**Description:** Rename a single column

**Scala Implementation:**
```scala
df.withColumnRenamed(oldName, newName)
```

**PySpark Equivalent:**
```python
df.withColumnRenamed(old_name, new_name)
```

**Complexity:** Low

---

### 3. AddColumnByExpr
**File:** `AddColumnByExpr.scala`

**Description:** Add a new column from SQL expression

**Scala Implementation:**
```scala
df.withColumn(colName, expr(expression))
```

**PySpark Equivalent:**
```python
df.withColumn(col_name, expr(expression))
```

**Complexity:** Low

---

### 4. AddColumnRenamed
**File:** `AddColumnRenamed.scala`

**Description:** Add a column as a renamed copy of an existing column

**Scala Implementation:**
```scala
df.withColumn(newName, col(oldName))
```

**PySpark Equivalent:**
```python
df.withColumn(new_name, col(old_name))
```

**Complexity:** Low

---

### 5. SQL
**File:** `SQL.scala`

**Description:** Execute SQL query on DataFrame (requires temp view)

**Scala Implementation:**
```scala
df.createOrReplaceTempView(table)
spark.sql(query)
```

**PySpark Equivalent:**
```python
df.createOrReplaceTempView(table)
spark.sql(query)
```

**Complexity:** Low

---

### 6. CreateTemporaryTable
**File:** `CreateTemporaryTable.scala`

**Description:** Create a temporary view for SQL operations

**Scala Implementation:**
```scala
df.createOrReplaceTempView(tableName)
```

**PySpark Equivalent:**
```python
df.createOrReplaceTempView(table_name)
```

**Complexity:** Low

---

### 7. AddWindowFunction
**File:** `AddWindowFunction.scala`

**Description:** Add window function column with partitioning and ordering

**Scala Implementation:**
```scala
Window.spec.partitionBy(col1, col2).orderBy(col3)
df.withColumn(colName, expr(func) over spec)
```

**PySpark Equivalent:**
```python
from pyspark.sql.window import Window
spec = Window.partitionBy(col1, col2).orderBy(col3)
df.withColumn(col_name, F.expr(func).over(spec))
```

**Complexity:** Medium

---

### 8. GroupByColumns
**File:** `GroupByColumns.scala`

**Description:** Group by columns with aggregations (extends GroupingTransformer)

**Scala Implementation:**
```scala
df.groupBy(cols.map(col): _*)
   .agg(aggregators: _*)
```

**PySpark Equivalent:**
```python
df.groupBy(cols).agg(*aggregators)
```

**Complexity:** Medium

---

### 9. JoinByExpr
**File:** `JoinByExpr.scala`

**Description:** Join DataFrames by SQL expression

**Scala Implementation:**
```scala
df.join(other, expr(joinExpr), joinType)
```

**PySpark Equivalent:**
```python
df.join(other, expr(join_expr), join_type)
```

**Complexity:** Medium

---

### 10. JoinById
**File:** `JoinById.scala`

**Description:** Join DataFrames by ID columns with optional suffix

**Scala Implementation:**
```scala
df.join(other, joinCols, joinType)
```

**PySpark Equivalent:**
```python
df.join(other, join_cols, join_type)
```

**Complexity:** Low

---

### 11. UnionTable
**File:** `UnionTable.scala`

**Description:** Union DataFrame with table, optional filter and latest data selection

**Scala Implementation:**
```scala
val table = spark.table(tableName)
if (filter.isDefined) table = table.filter(expr)
if (latestData) table = getLatestPartition(table)
table.union(df)
```

**PySpark Equivalent:**
```python
table = spark.table(table_name)
if filter_expression:
    table = table.filter(expr(filter_expression))
if latest_data:
    table = get_latest_partition(table)
table.union(df)
```

**Complexity:** High (requires partition handling)

---

### 12. AddDateDifference
**File:** `AddDateDifference.scala`

**Description:** Add date difference column between two dates

**Scala Implementation:**
```scala
df.withColumn(colName, datediff(endDate, startDate))
```

**PySpark Equivalent:**
```python
df.withColumn(col_name, F.datediff(end_date, start_date))
```

**Complexity:** Low

---

### 13. AddDate
**File:** `AddDate.scala`

**Description:** Add date-related columns (year, month, day, hour)

**Scala Implementation:**
```scala
df.withColumn(yearCol, year(col))
   .withColumn(monthCol, month(col))
   .withColumn(dayCol, dayofmonth(col))
   .withColumn(hourCol, hour(col))
```

**PySpark Equivalent:**
```python
df.withColumn(year_col, F.year(col)) \
  .withColumn(month_col, F.month(col)) \
  .withColumn(day_col, F.dayofmonth(col)) \
  .withColumn(hour_col, F.hour(col))
```

**Complexity:** Low

---

### 14. AddDayType
**File:** `AddDayType.scala`

**Description:** Add day type column (weekday/weekend)

**Scala Implementation:**
```scala
df.withColumn(colName,
  when(dayofweek(col).between(2, 6), "weekday")
  .otherwise("weekend")
)
```

**PySpark Equivalent:**
```python
df.withColumn(col_name,
    F.when(F.dayofweek(col).between(2, 6), "weekday")
     .otherwise("weekend")
)
```

**Complexity:** Low

---

### 15. AddWeek
**File:** `AddWeek.scala`

**Description:** Add week of year column

**Scala Implementation:**
```scala
df.withColumn(colName, weekofyear(col))
```

**PySpark Equivalent:**
```python
df.withColumn(col_name, F.weekofyear(col))
```

**Complexity:** Low

---

### 16. AddEntropy
**File:** `AddEntropy.scala`

**Description:** Add entropy calculation column (requires custom UDF)

**Scala Implementation:**
```scala
// Complex: groupBy, collect_list, UDF calculation
val entropyUDF = udf(calculateEntropy)
df.groupBy(groupCols).agg(sum(entity) as "entity_per_group")
   .groupBy(idCols).agg(collect_list("entity_per_group") as "list")
   .withColumn(entropyCol, entropyUDF(col("list")))
```

**PySpark Equivalent:**
```python
from pyspark.sql.functions import udf
@udf("double")
def calculate_entropy(entities):
    # Python entropy calculation
    ...
df.groupBy(group_cols).agg(F.sum(entity).alias("entity_per_group")) \
  .groupBy(id_cols).agg(F.collect_list("entity_per_group").alias("list")) \
  .withColumn(entropy_col, calculate_entropy(F.col("list")))
```

**Complexity:** High (complex UDF logic)

---

### 17. AddLatLongDistance
**File:** `AddLatLongDistance.scala`

**Description:** Add Haversine distance between two lat/long points (requires UDF)

**Scala Implementation:**
```scala
val haversineUDF = udf(calculateDistance)
df.withColumn(distCol, haversineUDF(lat1, lon1, lat2, lon2))
```

**PySpark Equivalent:**
```python
from pyspark.sql.functions import udf
import math

@udf("double")
def haversine_distance(lat1, lon1, lat2, lon2):
    # Python haversine calculation
    ...
df.withColumn(dist_col, haversine_distance(lat1, lon1, lat2, lon2))
```

**Complexity:** Medium (math UDF)

---

### 18. Coalesce
**File:** `Coalesce.scala`

**Description:** Coalesce multiple columns into one (first non-null)

**Scala Implementation:**
```scala
df.withColumn(outputCol, coalesce(cols: _*))
```

**PySpark Equivalent:**
```python
df.withColumn(output_col, F.coalesce(*cols))
```

**Complexity:** Low

---

### 19. CastColumn
**File:** `CastColumn.scala`

**Description:** Cast column to different data type

**Scala Implementation:**
```scala
df.withColumn(outputCol, col(inputCol).cast(dataType))
```

**PySpark Equivalent:**
```python
df.withColumn(output_col, F.col(input_col).cast(data_type))
```

**Complexity:** Low

---

### 20. TruncateDecimal
**File:** `TruncateDecimal.scala`

**Description:** Truncate decimal digits (requires UDF)

**Scala Implementation:**
```scala
val truncateUDF = udf((input: Double) =>
  BigDecimal(input).setScale(offset, rounding).toDouble
)
df.withColumn(outputCol, truncateUDF(col(inputCol)))
```

**PySpark Equivalent:**
```python
from pyspark.sql.functions import udf

@udf("double")
def truncate_decimal(input, offset, round_up):
    from decimal import Decimal, ROUND_UP, ROUND_DOWN
    rounding = ROUND_UP if round_up else ROUND_DOWN
    return float(Decimal(str(input)).quantize(
        Decimal('0.' + '0' * (offset - 1) + '1'),
        rounding=routing
    ))
df.withColumn(output_col, truncate_decimal(F.col(input_col), F.lit(offset), F.lit(round_up)))
```

**Complexity:** Medium (decimal UDF)

---

### 21. SelectColumns
**File:** `SelectColumns.scala`

**Description:** Select specific columns from DataFrame

**Scala Implementation:**
```scala
df.select(cols.map(col): _*)
```

**PySpark Equivalent:**
```python
df.select(*cols)
```

**Complexity:** Low

---

### 22. DropCols
**File:** `DropCols.scala`

**Description:** Drop specific columns from DataFrame

**Scala Implementation:**
```scala
df.drop(cols: _*)
```

**PySpark Equivalent:**
```python
df.drop(*cols)
```

**Complexity:** Low

---

### 23. DistinctAttributes
**File:** `DistinctAttributes.scala`

**Description:** Get distinct rows (drop duplicates)

**Scala Implementation:**
```scala
df.distinct()
# or
df.dropDuplicates(cols)
```

**PySpark Equivalent:**
```python
df.distinct()
# or
df.dropDuplicates(cols)
```

**Complexity:** Low

---

### 24. FilterByColumnValue
**File:** `FilterByColumnValue.scala`

**Description:** Filter rows by column value equality

**Scala Implementation:**
```scala
df.filter(col(colName) === value)
```

**PySpark Equivalent:**
```python
df.filter(F.col(col_name) == value)
```

**Complexity:** Low

---

### 25. FilterByIsIn
**File:** `FilterByIsIn.scala`

**Description:** Filter rows by column value in list

**Scala Implementation:**
```scala
df.filter(col(colName).isin(values: _*))
```

**PySpark Equivalent:**
```python
df.filter(F.col(col_name).isin(*values))
```

**Complexity:** Low

---

### 26. RegexFilter
**File:** `RegexFilter.scala`

**Description:** Filter rows by regex pattern match

**Scala Implementation:**
```scala
df.filter(col(inputCol).rlike(regex))
```

**PySpark Equivalent:**
```python
df.filter(F.col(input_col).rlike(regex))
```

**Complexity:** Low

---

### 27. Melt
**File:** `Melt.scala`

**Description:** Convert DataFrame from wide to long format (inverse of pivot)

**Scala Implementation:**
```scala
val varsAndVals = array(for (c <- inputCols) yield
  struct(lit(c).alias(keyCol), col(c).alias(valueCol))
)
df.withColumn("_vars_and_vals", explode(varsAndVals))
  .select(keyCols ++ List(keyCol, valueCol): _*)
```

**PySpark Equivalent:**
```python
# Complex melt operation
from pyspark.sql.functions import array, struct, explode, lit

vars_and_vals = F.array(*[
    F.struct(F.lit(c).alias(key_col), F.col(c).alias(value_col))
    for c in input_cols
])

df.withColumn("_vars_and_vals", F.explode(vars_and_vals)) \
  .select(*key_cols,
          F.col("_vars_and_vals." + key_col),
          F.col("_vars_and_vals." + value_col))
```

**Complexity:** High (complex struct/explode logic)

---

### 28. StructAssembler
**File:** `StructAssembler.scala`

**Description:** Combine multiple columns into a single struct column

**Scala Implementation:**
```scala
val targetCols = if (excludeCols.isDefined)
  dataset.columns.filterNot(excludeCols.contains)
else inputCols

df.withColumn(outputCol, struct(targetCols.map(col): _*))
```

**PySpark Equivalent:**
```python
if exclude_cols:
    target_cols = [c for c in df.columns if c not in exclude_cols]
else:
    target_cols = input_cols

df.withColumn(output_col, F.struct(*target_cols))
```

**Complexity:** Low

---

### 29. ColumnValueRenamed
**File:** `ColumnValueRenamed.scala`

**Description:** Rename specific values in a column

**Scala Implementation:**
```scala
df.withColumn(colName,
  when(col(colName) === oldValue, newValue)
  .otherwise(col(colName))
)
```

**PySpark Equivalent:**
```python
df.withColumn(col_name,
    F.when(F.col(col_name) == old_value, new_value)
     .otherwise(F.col(col_name))
)
```

**Complexity:** Low

---

### 30. BaseTransformer
**File:** `BaseTransformer.scala`

**Description:** Abstract base class for all transformers

**Scala Implementation:**
```scala
abstract class BaseTransformer(uid: String) extends Transformer {
  def transformSchema(schema: StructType): StructType = schema
  def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

abstract class GroupingTransformer extends BaseTransformer {
  def withAggregators(aggregators: Seq[Column]): GroupingTransformer
}
```

**PySpark Equivalent:**
```python
from abc import ABC, abstractmethod

class BaseTransformer(ABC):
    def __init__(self, uid: str = None):
        self.uid = uid or self._random_uid()

    @abstractmethod
    def transform(self, df):
        pass

class GroupingTransformer(BaseTransformer):
    def withAggregators(self, aggregators):
        return self
```

**Complexity:** N/A (base class)

---

## Summary Statistics

- **Total Transformers:** 30
- **Low Complexity:** 19 (simple column operations)
- **Medium Complexity:** 6 (window functions, UDFs, joins)
- **High Complexity:** 5 (complex logic: UnionTable, AddEntropy, Melt, etc.)

## Complexity Breakdown

### Low Complexity (19)
Direct PySpark function mappings:
1. FilterByExpr
2. ColumnRenamed
3. AddColumnByExpr
4. AddColumnRenamed
5. SQL
6. CreateTemporaryTable
7. JoinById
8. AddDateDifference
9. AddDate
10. AddDayType
11. AddWeek
12. Coalesce
13. CastColumn
14. SelectColumns
15. DropCols
16. DistinctAttributes
17. FilterByColumnValue
18. FilterByIsIn
19. RegexFilter

### Medium Complexity (6)
Require custom UDFs or window functions:
1. AddWindowFunction (window specs)
2. GroupByColumns (dynamic aggregations)
3. JoinByExpr (expression joins)
4. AddLatLongDistance (math UDF)
5. TruncateDecimal (decimal UDF)
6. StructAssembler (struct manipulation)

### High Complexity (5)
Require complex multi-step logic:
1. AddEntropy (complex UDF with groupBy + collect_list)
2. Melt (struct + explode transformation)
3. UnionTable (partition handling + latest data logic)
4. BaseTransformer (architecture design)
5. ColumnValueRenamed (when/otherwise logic)

---

## Implementation Priority

### Phase 1: Low Complexity (Week 1)
- All 19 simple transformers
- Goal: Quick wins, establish patterns

### Phase 2: Medium Complexity (Week 2)
- 6 medium transformers
- Goal: UDF patterns, window functions

### Phase 3: High Complexity (Week 3)
- 5 high-complexity transformers
- Goal: Complex transformations, architecture

---

## Notes

1. **UDF Porting:** Scala UDFs need Python equivalents with `@udf` decorator
2. **Type Safety:** PySpark is less type-safe than Scala, need validation
3. **Testing:** Each transformer needs comprehensive unit tests
4. **Performance:** Python UDFs are slower than Scala, consider pandas UDF for performance
5. **Backward Compatibility:** API should match Scala version for migration path

---

## References

- **Scala Source:** `engines/spark-engine/src/main/scala/tech/mta/seeknal/transformers/`
- **PySpark Target:** `src/seeknal/tasks/sparkengine/transformers/`
- **PySpark Docs:** https://spark.apache.org/docs/latest/api/python/
- **Migration Guide:** See Task #3 (Test Infrastructure)
