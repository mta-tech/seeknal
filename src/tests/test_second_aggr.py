from __future__ import division
import pytest
from seeknal.tasks.sparkengine.aggregators import SecondOrderAggregator, AggregationSpec


@pytest.fixture
def input_df(request, spark):
    input_data = [
        (1, None, "2019/01/10", "2019-01-20", "b"),
        (1, 2, "2019/01/13", "2019-01-20", None),
        (1, 2, "2019/01/15", "2019-01-20", "a"),
        (1, 2, "2019/01/16", "2019-01-20", "a"),
        (1, 3, "2019/01/14", "2019-01-20", "a"),
        (1, 10, "2019/01/21", "2019-01-20", "b"),
        (2, 4, "2019/01/01", "2019-01-23", "a"),
        (2, 3, "2019/01/02", "2019-01-23", None),
        (2, 3, "2019/01/03", "2019-01-23", "a"),
        (2, None, "2019/01/12", "2019-01-23", "c"),
        (2, 2, "2019/01/18", "2019-01-23", "a"),
        (2, None, "2019/01/15", "2019-01-23", "a"),
        (3, None, "2019/01/22", "2019-01-30", "c"),
    ]
    return spark.createDataFrame(input_data, ["id", "feature1", "f_date", "a_date", "feature2"])

application_date_format = "yyyy-MM-dd"
feature_date_format = "yyyy/MM/dd"
aggregators = SecondOrderAggregator(
    idCol="id", applicationDateCol="a_date", featureDateCol="f_date", applicationDateFormat=application_date_format, featureDateFormat=feature_date_format
)
methods = {
    "count": "count",
    "mean": "mean",
    "min": "min",
    "max": "max",
    "std": "stddev",
    "sum": "sum",
    "random_value": "first",
    "since": "since",
    "count_distinct": "approx_count_distinct",
    "sum_distinct": "sumDistinct",
}

def test_accumulator_basic(input_df, spark):
    expected_output_data = [
        (1, 5, "a", 2, "a", 5, 3.8, 2, 10, 3.492849839314596, 19, 3, 3, 15, 2, 1.3709505796432495),
        (3, 1, "c", 1, "c", 0, None, None, None, None, None, None, 0, None, None, None),
        (2, 5, None, 2, "a", 4, 3.0, 2, 4, 0.816496580927726, 12, 3, 3, 9, 3, 1.5),
    ]
    output_col = [
        "id",
        "feature2_COUNT",
        "feature2_RANDOM_VALUE",
        "feature2_COUNT_DISTINCT",
        "feature2_MOST_FREQUENT",
        "feature1_COUNT",
        "feature1_MEAN",
        "feature1_MIN",
        "feature1_MAX",
        "feature1_STD",
        "feature1_SUM",
        "feature1_RANDOM_VALUE",
        "feature1_COUNT_DISTINCT",
        "feature1_SUM_DISTINCT",
        "feature1_MOST_FREQUENT",
        "feature1_ENTROPY",
    ]
    expected_output_df = spark.createDataFrame(expected_output_data, output_col)

    rules = [
        # categorical feature2
        AggregationSpec("basic", "feature2", "count, random_value, count_distinct, most_frequent"),
        # numeric feature1
        AggregationSpec("basic", "feature1", "count, mean, min, max, std, sum, random_value, count_distinct, sum_distinct, most_frequent, entropy"),
    ]
    output_df = aggregators.setRules(rules).transform(input_df)

    # only compare deterministic columns
    nd_cols = ["feature2_RANDOM_VALUE", "feature1_RANDOM_VALUE"]
    output_df.show()

def test_rule_one_each_kind(input_df, spark):
    expected_output_data = [
        (1, 3.0, 1.0, 3.0, 5, "a", 5, 2, 4, "a", 4, 2, 4, 2.0, 4, 2.0),
        (3, 0.0, None, None, 1, "c", 0, None, 1, "c", 0, None, 1, 0.0, 0, None),
        (2, 0.25, 0.6666666666666666, 0.3333333333333333, 5, "a", 4, 3, 2, "a", 2, 2, 5, 2.321928024291992, 4, 2.0),
    ]
    expected_output_columns = [
        "id",
        "feature2_COUNT1_6_COUNT7_25",
        "feature1_MOST_FREQUENT1_6_MOST_FREQUENT7_25",
        "feature1_COUNT1_6_COUNT7_25",
        "feature2_COUNT",
        "feature2_MOST_FREQUENT",
        "feature1_COUNT",
        "feature1_MOST_FREQUENT",
        "feature2_COUNT_1_10",
        "feature2_MOST_FREQUENT_1_10",
        "feature1_COUNT_1_20",
        "feature1_MOST_FREQUENT_1_10",
        "SINCE_COUNT_feature2_GEO_D",
        "SINCE_ENTROPY_feature2_GEO_D",
        "SINCE_COUNT_feature1_GEO_D",
        "SINCE_ENTROPY_feature1_GEO_D",
    ]
    expected_output_df = spark.createDataFrame(expected_output_data, expected_output_columns)
    rules = [
        AggregationSpec("ratio", "feature2", "count", "", 1, 6, 7, 25),
        AggregationSpec("ratio", "feature1", "most_frequent", "", 1, 6, 7, 25),
        AggregationSpec("ratio", "feature1", "count", "", 1, 6, 7, 25),
        AggregationSpec("basic", "feature2", "count"),
        AggregationSpec("basic", "feature2", "most_frequent"),
        AggregationSpec("basic", "feature1", "count"),
        AggregationSpec("basic", "feature1", "most_frequent"),
        AggregationSpec("basic_days", "feature2", "count", "", 1, 10),
        AggregationSpec("basic_days", "feature2", "most_frequent", "", 1, 10),
        AggregationSpec("basic_days", "feature1", "count", "", 1, 20),
        AggregationSpec("basic_days", "feature1", "most_frequent", "", 1, 10),
        AggregationSpec(
            "since",
            "feature2",
            "count",
        ),
        AggregationSpec(
            "since",
            "feature2",
            "entropy",
        ),
        AggregationSpec(
            "since",
            "feature1",
            "count",
        ),
        AggregationSpec(
            "since",
            "feature1",
            "entropy",
        ),
    ]
    output_df = aggregators.setRules(rules).transform(input_df)
    output_df.show()
