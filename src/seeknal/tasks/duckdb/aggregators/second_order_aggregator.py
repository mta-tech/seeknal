from collections import namedtuple
from typing import List, Optional, Union
import duckdb

class AggregationSpec(
    namedtuple(
        "AggregationSpec",
        [
            "name",
            "features",
            "aggregations",
            "filterCondition",
            "dayLimitLower1",
            "dayLimitUpper1",
            "dayLimitLower2",
            "dayLimitUpper2",
        ],
    )
):
    def __new__(
        cls,
        name,
        features,
        aggregations,
        filterCondition="",
        dayLimitLower1="",
        dayLimitUpper1="",
        dayLimitLower2="",
        dayLimitUpper2="",
    ):
        return super(AggregationSpec, cls).__new__(
            cls,
            name,
            features,
            aggregations,
            filterCondition,
            dayLimitLower1,
            dayLimitUpper1,
            dayLimitLower2,
            dayLimitUpper2,
        )

class SecondOrderAggregator:
    """
    Performs aggregations on features based on the input rules using DuckDB.

    This class enables feature engineering by creating second-order features from transaction-level or event-level data.
    It supports various aggregation strategies that can be combined to generate a rich feature set.

    Supported Aggregation Types:
        1. **basic**: Simple aggregation over the entire history per ID.
           - Format: `AggregationSpec("basic", "feature_col", "agg_func")`
           - Example: `AggregationSpec("basic", "amount", "sum")` -> Sum of amount for each user.
        
        2. **basic_days**: Aggregation over a specific time window defined by `days_between` (time since application date).
           - Format: `AggregationSpec("basic_days", "feature_col", "agg_func", "", lower, upper)`
           - Example: `AggregationSpec("basic_days", "amount", "mean", "", 1, 30)` -> Mean amount in the last 1-30 days.

        3. **ratio**: Ratio of two aggregations over different time windows.
           - Format: `AggregationSpec("ratio", "feature_col", "agg_func", "", lower1, upper1, lower2, upper2)`
           - Example: `AggregationSpec("ratio", "amount", "sum", "", 1, 30, 31, 60)` -> (Sum 1-30 days) / (Sum 31-60 days).

        4. **since**: Aggregation filtered by a custom condition.
           - Format: `AggregationSpec("since", "feature_col", "agg_func", "condition")`
           - Example: `AggregationSpec("since", "flag", "count", "flag == 1")` -> Count of flag=1 events.

    The `transform` method generates a SQL query based on these rules and executes it against a DuckDB relation or table.
    """

    def __init__(
        self,
        idCol=None,
        featureDateCol=None,
        featureDateFormat="yyyy-MM-dd",
        applicationDateCol=None,
        applicationDateFormat="yyyy-MM-dd",
        conn=None
    ):
        """
        Initialize the SecondOrderAggregator.

        :param idCol: Column name representing the entity ID (e.g., 'user_id', 'msisdn').
        :param featureDateCol: Column name representing the date of the event/transaction.
        :param featureDateFormat: Format of the feature date column (default: "yyyy-MM-dd").
        :param applicationDateCol: Column name representing the reference date (e.g., application date) to calculate 'days_between'.
        :param applicationDateFormat: Format of the application date column (default: "yyyy-MM-dd").
        :param conn: Optional existing DuckDB connection. If None, a new in-memory connection is created.
        """
        self.idCol = idCol
        self.featureDateCol = featureDateCol
        self.featureDateFormat = featureDateFormat
        self.applicationDateCol = applicationDateCol
        self.applicationDateFormat = applicationDateFormat
        self.rules = []
        self.conn = conn if conn else duckdb.connect()

    def setRules(self, rules):
        self.rules = rules
        return self

    def transform(self, table_name: str) -> duckdb.DuckDBPyRelation:
        """
        Transforms the input table using the defined aggregation rules.
        """
        if not self.rules:
            raise ValueError("No rules defined for aggregation.")

        # 1. Generate CTE for days_between calculation
        # We calculate it once as `_days_between`
        days_between_expr = f"date_diff('day', CAST({self._quote(self.featureDateCol)} AS DATE), CAST({self._quote(self.applicationDateCol)} AS DATE))"
        
        # Build the select clauses for the main query
        # Since we are querying from the CTE, we don't need to repeat the date_diff logic
        # But we do need to reference `_days_between` in filters.
        
        select_clauses = [self._quote(self.idCol)]
        
        for rule in self.rules:
            features = [f.strip() for f in rule.features.split(",")]
            aggs = [a.strip() for a in rule.aggregations.split(",")]
            
            for feature in features:
                for agg in aggs:
                    agg_func = self._map_agg_func(agg)
                    
                    if rule.name == "basic":
                        select_clauses.append(self._build_basic_agg_expr(feature, agg, agg_func))
                        
                    elif rule.name == "basic_days":
                        select_clauses.append(self._build_days_agg_expr(feature, agg, agg_func, rule))
                        
                    elif rule.name == "ratio":
                        select_clauses.append(self._build_ratio_agg_expr(feature, agg, agg_func, rule))

                    elif rule.name == "since":
                        select_clauses.append(self._build_since_agg_expr(feature, agg, agg_func, rule, days_between_expr))

        # 2. Construct the final query with CTE
        cte_query = f"""
        WITH calcs AS (
            SELECT 
                *,
                {days_between_expr} AS _days_between
            FROM {table_name}
        )
        SELECT 
            {', '.join(select_clauses)}
        FROM calcs
        GROUP BY {self._quote(self.idCol)}
        """
        
        return self.conn.sql(cte_query)

    def _quote(self, identifier: str) -> str:
        """Securely quote SQL identifiers."""
        # Escape double quotes by doubling them
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'

    def _build_basic_agg_expr(self, feature: str, agg: str, agg_func: str) -> str:
        col_alias = self._quote(f"{feature}_{agg.upper()}")
        return f"{agg_func}({self._quote(feature)}) AS {col_alias}"

    def _build_days_agg_expr(self, feature: str, agg: str, agg_func: str, rule) -> str:
        lower, upper = rule.dayLimitLower1, rule.dayLimitUpper1
        col_alias = self._quote(f"{feature}_{agg.upper()}_{lower}_{upper}")
        condition = f"_days_between BETWEEN {lower} AND {upper}"
        return f"{agg_func}({self._quote(feature)}) FILTER (WHERE {condition}) AS {col_alias}"

    def _build_ratio_agg_expr(self, feature: str, agg: str, agg_func: str, rule) -> str:
        l1, u1 = rule.dayLimitLower1, rule.dayLimitUpper1
        l2, u2 = rule.dayLimitLower2, rule.dayLimitUpper2
        
        col_alias = self._quote(f"{feature}_{agg.upper()}{l1}_{u1}_{agg.upper()}{l2}_{u2}")
        
        cond1 = f"_days_between BETWEEN {l1} AND {u1}"
        cond2 = f"_days_between BETWEEN {l2} AND {u2}"
        
        term1 = f"{agg_func}({self._quote(feature)}) FILTER (WHERE {cond1})"
        term2 = f"{agg_func}({self._quote(feature)}) FILTER (WHERE {cond2})"
        
        return f"CAST({term1} AS DOUBLE) / NULLIF(CAST({term2} AS DOUBLE), 0) AS {col_alias}"

    def _build_since_agg_expr(self, feature: str, agg: str, agg_func: str, rule, days_between_expr) -> str:
        # Note: 'since' conditions often reference raw columns, so we can't easily rely on just _days_between
        # unless parsing the condition. However, if the condition is just logic on `days_between`, 
        # we can replace `days_between_expr` with `_days_between` IF we know the format.
        # But `rule.filterCondition` might be "LIMIT_INCREASE_FLAG == 1" which doesn't use days_between.
        
        # Original logic: replace "==" with "="
        clean_cond = rule.filterCondition.replace("==", "=") if rule.filterCondition else ""
        if not clean_cond:
             # Default: days_between >= 0. Here we can use our cached column.
             clean_cond = f"_days_between >= 0"
        
        # Naming convention
        col_alias = self._quote(f"SINCE_{agg.upper()}_{feature}_GEO_D")
        
        return f"{agg_func}({self._quote(feature)}) FILTER (WHERE {clean_cond}) AS {col_alias}"

    def validate(self, table_name: str) -> List[str]:
        """
        Validates that the input table has the required columns.
        Returns a list of error messages.
        """
        errors = []
        try:
            # Fetch table schema. Using limit 0 to just get columns efficiently.
            # Using describe or pragma table_info might be better but simple select works too.
            # Or use self.conn.table(table_name).columns if available.
            columns = set(self.conn.table(table_name).columns)
        except Exception as e:
            return [f"Could not inspect table '{table_name}': {str(e)}"]

        if self.idCol not in columns:
            errors.append(f"Missing ID column: '{self.idCol}'")
            
        used_features = set()
        for rule in self.rules:
            # features can be comma separated
            for f in rule.features.split(','):
                used_features.add(f.strip())
                
        # Check date columns if they are not derived/expression-based?
        # self.featureDateCol might be complex expression? 
        # Usually checking exact column name existence.
        if self.featureDateCol not in columns:
             # It might be fine if it's an expression in actual usage, but for simple col checking:
             errors.append(f"Missing feature date column: '{self.featureDateCol}'")
             
        if self.applicationDateCol not in columns:
             errors.append(f"Missing application date column: '{self.applicationDateCol}'")

        missing = used_features - columns
        if missing:
            errors.append(f"Missing feature columns: {missing}")
            
        return errors

    def builder(self):
        """Returns a FeatureBuilder instance for this aggregator."""
        return FeatureBuilder(self)

    def _map_agg_func(self, agg: str) -> str:
        mapping = {
            "count": "count",
            "sum": "sum",
            "mean": "avg",
            "avg": "avg",
            "min": "min",
            "max": "max",
            "std": "stddev",
            "stddev": "stddev"
        }
        return mapping.get(agg.lower(), agg)


class FeatureBuilder:
    """
    Fluent builder for SecondOrderAggregator rules.
    """
    def __init__(self, aggregator):
        self.aggregator = aggregator
        self.current_feature = None

    def feature(self, name: str):
        self.current_feature = name
        return self

    def basic(self, aggs: List[str]):
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for agg in aggs:
            self.aggregator.rules.append(AggregationSpec("basic", self.current_feature, agg))
        return self

    def rolling(self, days: List[tuple], aggs: List[str]):
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for lower, upper in days:
            for agg in aggs:
                self.aggregator.rules.append(
                    AggregationSpec("basic_days", self.current_feature, agg, "", lower, upper)
                )
        return self
        
    def ratio(self, numerator: tuple, denominator: tuple, aggs: List[str]):
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        l1, u1 = numerator
        l2, u2 = denominator
        for agg in aggs:
            self.aggregator.rules.append(
                AggregationSpec("ratio", self.current_feature, agg, "", l1, u1, l2, u2)
            )
        return self

    def since(self, condition: str, aggs: List[str]):
        if not self.current_feature:
            raise ValueError("Call .feature() before defining aggregations.")
        for agg in aggs:
            self.aggregator.rules.append(
                AggregationSpec("since", self.current_feature, agg, condition)
            )
        return self

    def build(self):
        return self.aggregator
