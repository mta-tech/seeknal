# encoding=utf8
from __future__ import print_function
from __future__ import division
from builtins import str
from collections import namedtuple

import pyspark.sql.functions as F
from pyspark import keyword_only, SparkContext
from pyspark.ml import Transformer
from pyspark.ml.param import Param, Params
from pyspark.sql import Column

import math
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable, DefaultParamsReader, DefaultParamsWriter  # noq
import pyspark.sql.types as T

# constants
FUNCTION = "func"
AGG_NAME = "agg_name"
TYPE = "type"
SINCE = "SINCE"
GEO_D = "GEO_D"
DATE = "date"
NUMERATOR = "n"
DENOMINATOR = "d"
DAYS_BETWEEN = "days_between"

# all aggregations, values are the names of internal scala spark functions
all_native_aggregations = {
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

all_udf_aggregations = {
    "entropy": "entropy_cal_udf",
    "first_value": "first_val_udf",
    "last_value": "last_val_udf",
    "most_frequent": "most_frequent_udf",
    "changes_hist": "changes_hist_udf",
}

class Serialisable(DefaultParamsReadable, DefaultParamsWritable, Params):
    pass

def generate_day_cond(agg_col_name, days_between, dayLimitLower, dayLimitUpper, cond=None):
    """
    generate list of filter condition for column aggregate tranformation process
    :param agg_col_name: column to be transformed
    :param dayLimitLower1:
    :param dayLimitUpper1:
    :param day_col_name: day column name
    :param days_between: day range between application date and feature date
    :param cond: filter condition, 'feature1 == 1'
    :return: filter condition
    """
    if cond is not None:
        if (dayLimitLower is not None) and (dayLimitUpper is not None):
            return F.when(
                (F.col(days_between) >= dayLimitLower) & (F.col(days_between) <= dayLimitUpper) & (F.col(agg_col_name).isNotNull() & (cond)),
                F.col(agg_col_name),
            )
        elif days_between is None:
            return F.when(
                (F.col(agg_col_name).isNotNull() & (cond)),
                F.col(agg_col_name),
            )
        elif days_between is not None:
            return F.when(
                ((F.col(agg_col_name).isNotNull()) & (cond)),
                F.col(days_between),
            )
    else:
        if (dayLimitLower is not None) and (dayLimitUpper is not None):
            return F.when(
                (F.col(days_between) >= dayLimitLower) & (F.col(days_between) <= dayLimitUpper) & (F.col(agg_col_name).isNotNull()),
                F.col(agg_col_name),
            )
        elif days_between is None:
            return F.when(
                (F.col(agg_col_name).isNotNull()),
                F.col(agg_col_name),
            )
        elif days_between is not None:
            return F.when(
                ((F.col(agg_col_name).isNotNull())),
                F.col(days_between),
            )


def generate_aggregation_expressions(methods, functions, agg_col_names, dayLimitLower, dayLimitUpper, days_between, conds=None):
    """
    generate list of aggregate expression
    :param methods: list of aggregate function method
    :param functions: list of aggregate function name
    :param agg_col_names: list of columns to be transformed
    :param dayLimitLower1:
    :param dayLimitUpper1:
    :param day_col_name: day column name
    :param days_between: day range between application date and feature date
    :param conds: filter condition, 'feature1 == 1'
    :return: a list of aggregate expression
    """
    if conds is not None:
        return [
            methods[func.strip()](generate_day_cond(t[0], days_between, dayLimitLower, dayLimitUpper, t[1])).alias(
                "{}_{}_{}_{}".format(t[0], func.strip().upper(), dayLimitLower, dayLimitUpper)
                if (dayLimitLower is not None) and (dayLimitUpper is not None)
                else "{}_{}".format(t[0], func.strip().upper())
            )
            for t in tuple(zip(agg_col_names, conds))
            for func in functions
        ]
    else:
        return [
            methods[func.strip()](generate_day_cond(agg_col_name, days_between, dayLimitLower, dayLimitUpper)).alias(
                "{}_{}_{}_{}".format(agg_col_name, func.strip().upper(), dayLimitLower, dayLimitUpper)
                if (dayLimitLower is not None) and (dayLimitUpper is not None)
                else "{}_{}".format(agg_col_name, func.strip().upper())
            )
            for agg_col_name in agg_col_names
            for func in functions
        ]


def generate_udf_aggregation_expressions(methods, functions, agg_col_names, dayLimitLower, dayLimitUpper, day_col_name, days_between, conds=None):
    """
    generate list of udf aggregate expression
    :param methods: list of aggregate function method
    :param functions: list of aggregate function name
    :param agg_col_names: list of columns to be transformed
    :param dayLimitLower1:
    :param dayLimitUpper1:
    :param day_col_name: day column name
    :param days_between: day range between application date and feature date
    :param conds: filter condition, 'feature1 == 1'
    :return: a list of aggregate expression
    """
    agg_exps = []
    if conds is not None:
        for func in functions:
            if func in ["entropy", "most_frequent"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](F.collect_list(generate_day_cond(t[0], days_between, dayLimitLower, dayLimitUpper, t[1]))).alias(
                        "{}_{}_{}_{}".format(t[0], func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) and (dayLimitUpper is not None)
                        else "{}_{}".format(t[0], func.strip().upper())
                    )
                    for t in tuple(zip(agg_col_names, conds))
                ]
            elif func in ["first_value", "last_value"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](
                        F.array_sort(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(day_col_name),
                                    generate_day_cond(t[0], days_between, dayLimitLower, dayLimitUpper, t[1]),
                                )
                            )
                        )
                    ).alias(
                        "{}_{}_{}_{}".format(t[0], func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) and (dayLimitUpper is not None)
                        else "{}_{}".format(t[0], func.strip().upper())
                    )
                    for t in tuple(zip(agg_col_names, conds))
                ]
            elif func in ["changes_hist"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](
                        F.struct(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(days_between),
                                    generate_day_cond(t[0], days_between, dayLimitLower, dayLimitUpper, t[1]),
                                )
                            ).alias("list"),
                            F.lit(dayLimitLower).alias("lower"),
                            F.lit(dayLimitUpper).alias("upper"),
                        )
                    ).alias(
                        "{}_{}_{}_{}".format(t[0], func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) and (dayLimitUpper is not None)
                        else "{}_{}".format(t[0], func.strip().upper())
                    )
                    for t in tuple(zip(agg_col_names, conds))
                ]
        return agg_exps
    else:
        for func in functions:
            if func in ["entropy", "most_frequent"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](F.collect_list(generate_day_cond(agg_col_name, days_between, dayLimitLower, dayLimitUpper, None))).alias(
                        "{}_{}_{}_{}".format(agg_col_name, func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) or (dayLimitUpper is not None)
                        else "{}_{}".format(agg_col_name, func.strip().upper())
                    )
                    for agg_col_name in agg_col_names
                ]
            elif func in ["first_value", "last_value"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](
                        F.array_sort(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(day_col_name),
                                    generate_day_cond(agg_col_name, days_between, dayLimitLower, dayLimitUpper, None),
                                )
                            )
                        )
                    ).alias(
                        "{}_{}_{}_{}".format(agg_col_name, func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) and (dayLimitUpper is not None)
                        else "{}_{}".format(agg_col_name, func.strip().upper())
                    )
                    for agg_col_name in agg_col_names
                ]
            elif func in ["changes_hist"]:
                agg_exps = agg_exps + [
                    methods[func.strip()](
                        F.struct(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(days_between),
                                    generate_day_cond(agg_col_name, days_between, dayLimitLower, dayLimitUpper, None),
                                )
                            ).alias("list"),
                            F.lit(dayLimitLower).alias("lower"),
                            F.lit(dayLimitUpper).alias("upper"),
                        )
                    ).alias(
                        "{}_{}_{}_{}".format(agg_col_name, func.strip().upper(), dayLimitLower, dayLimitUpper)
                        if (dayLimitLower is not None) or (dayLimitUpper is not None)
                        else "{}_{}".format(agg_col_name, func.strip().upper())
                    )
                    for agg_col_name in agg_col_names
                ]
        return agg_exps


def generate_ratio_aggregation_expressions(
    methods, functions, agg_col_names, dayLimitLower1, dayLimitUpper1, dayLimitLower2, dayLimitUpper2, days_between, conds=None
):
    """
    generate list of aggregate expression for ratio [feature between dayLimitUpper2 and dayLimitLower2]/[feature between dayLimitUpper1 and dayLimitLower1]
    :param methods: list of aggregate function method
    :param functions: list of aggregate function name
    :param agg_col_names: list of columns to be transformed
    :param dayLimitLower1:
    :param dayLimitUpper1:
    :param dayLimitLower2:
    :param dayLimitUpper2:
    :param day_col_name: day column name
    :param days_between: day range between application date and feature date
    :return: a list of aggregate expression
    """
    return [
        (
            methods[func.strip()](generate_day_cond(agg_col_name, days_between, dayLimitLower1, dayLimitUpper1, conds))
            / methods[func.strip()](generate_day_cond(agg_col_name, days_between, dayLimitLower2, dayLimitUpper2, conds))
        ).alias(
            "{}_{}{}_{}_{}{}_{}".format(
                agg_col_name,
                func.strip().upper(),
                str(dayLimitLower1),
                str(dayLimitUpper1),
                func.strip().upper(),
                str(dayLimitLower2),
                str(dayLimitUpper2),
            )
        )
        for agg_col_name in agg_col_names
        for func in functions
    ]


# generate udf aggregate expression for ratio
def generate_ratio_udf_aggregation_expressions(
    methods, functions, agg_col_names, dayLimitLower1, dayLimitUpper1, dayLimitLower2, dayLimitUpper2, day_col_name, days_between, conds=None
):
    """
    generate list of udf aggregate expression for ratio [feature between dayLimitUpper2 and dayLimitLower2]/[feature between dayLimitUpper1 and dayLimitLower1]
    :param methods: list of aggregate function method
    :param functions: list of aggregate function name
    :param agg_col_names: list of columns to be transformed
    :param dayLimitLower1:
    :param dayLimitUpper1:
    :param dayLimitLower2:
    :param dayLimitUpper2:
    :param day_col_name: day column name
    :param days_between: day range between application date and feature date
    :param conds: filter condition, 'feature1 == 1'
    :return: a list of aggregate expression
    """
    agg_exps = []
    for func in functions:
        if func in ["entropy", "most_frequent"]:
            agg_exps = agg_exps + [
                (
                    methods[func.strip()](F.collect_list(generate_day_cond(agg_col_name, days_between, dayLimitLower1, dayLimitUpper1, conds)))
                    / methods[func.strip()](F.collect_list(generate_day_cond(agg_col_name, days_between, dayLimitLower2, dayLimitUpper2, conds)))
                ).alias(
                    "{}_{}{}_{}_{}{}_{}".format(
                        agg_col_name,
                        func.strip().upper(),
                        str(dayLimitLower1),
                        str(dayLimitUpper1),
                        func.strip().upper(),
                        str(dayLimitLower2),
                        str(dayLimitUpper2),
                    )
                )
                for agg_col_name in agg_col_names
            ]
        elif func in ["first_value", "last_value"]:
            agg_exps = agg_exps + [
                (
                    methods[func.strip()](
                        F.array_sort(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(day_col_name),
                                    generate_day_cond(agg_col_name, days_between, dayLimitLower1, dayLimitUpper1, conds),
                                )
                            )
                        )
                    )
                    / methods[func.strip()](
                        F.array_sort(
                            F.collect_list(
                                F.concat_ws(
                                    "_|_",
                                    F.col(day_col_name),
                                    generate_day_cond(agg_col_name, days_between, dayLimitLower2, dayLimitUpper2, conds),
                                )
                            )
                        )
                    )
                ).alias(
                    "{}_{}{}_{}_{}{}_{}".format(
                        agg_col_name,
                        func.strip().upper(),
                        str(dayLimitLower1),
                        str(dayLimitUpper1),
                        func.strip().upper(),
                        str(dayLimitLower2),
                        str(dayLimitUpper2),
                    )
                )
                for agg_col_name in agg_col_names
            ]
    return agg_exps


# filter functions present in all_aggregations
def filter_aggregations(functions, all_aggregations):
    return [func.strip() for func in functions.split(",") if func.strip() in all_aggregations]


# udf calculate entropy
def entropy_cal(x):
    count = {}
    total = len(x)
    for i in x:
        count[i] = count.get(i, 0) + 1
    for i in count.keys():
        count[i] = -(count[i] / total) * math.log(count[i] / total, 2)
    return sum(count.values())


entropy_cal_udf = F.udf(entropy_cal, T.FloatType())


# udf calculate first val
def first_val(x):
    for i in x:
        if i.find("_|_") > 0:
            return i[i.find("_|_") + 3 : :]


first_val_udf = F.udf(first_val, T.StringType())


# udf calculate last val
def last_val(x):
    for i in reversed(x):
        if i.find("_|_") > 0:
            return i[i.find("_|_") + 3 : :]


last_val_udf = F.udf(last_val, T.StringType())


# udf calculate most frequent val
def most_frequent(x):
    count = {}
    for i in x:
        count[i] = count.get(i, 0) + 1
    if len(count) > 0:
        mf_key = max(count, key=count.get)
        return mf_key


most_frequent_udf = F.udf(most_frequent, T.StringType())


def changes_hist(x):
    """
    encoding a time series changes record/information into a string value which tracking the
    this function is only applicable to "basic_days"
    null and nan were considered equal in this function
    Example
    :a_date: target event date (loan application date)
    :f_date: event date
    +---+--------+--------+----------+----------+
    |id |feature1|feature2|f_date    |a_date    |
    +---+--------+--------+----------+----------+
    |1  |null    |a       |2020-01-23|2020-01-31|
    |1  |null    |b       |2020-01-24|2020-01-31|
    |1  |null    |b       |2020-01-24|2020-01-31|
    |1  |5       |null    |2020-01-25|2020-01-31|
    |1  |5       |null    |2020-01-26|2020-01-31|
    |1  |null    |a       |2020-01-27|2020-01-31|
    |1  |null    |a       |2020-01-27|2020-01-31|
    |1  |5       |a       |2020-01-28|2020-01-31|
    |1  |4       |a       |2020-01-29|2020-01-31|
    |1  |5       |b       |2020-01-30|2020-01-31|
    +---+--------+--------+----------+----------+
    logic (for n >= 0)
    for numeric features, features value will be accumlated by date
    for categorical/string features, feature's number of record will be sum up given it is not null or nan
        (string value doesnt vary the resut as it is just counting of event as long as not null, len([a, b]) == len([a, a]) )
    :">": if feature (as at a_date -n) is not null and feature (as at a_date -n -1) is null
    :"<": if feature (as at a_date -n) is null and feature (as at a_date -n -1) is not null
    :".": if feature both (as at a_date -n) and (as at a_date -n -1) are null
    :"_": if feature both (as at a_date -n) and (as at a_date -n -1) are not null and value are equal
    :"1": if feature (as at a_date -n) is smaller than feature (as at a_date -n -1)
    :"0": if feature (as at a_date -n) is bigger than feature (as at a_date -n -1)
    :return: Feature1_CHANGES_HIST_1_8 = ".<_><01"
    :return: Feature2_CHANGES_HIST_1_8 = "1>.<0__" (for categorical/string feature it is counting of events changes by record)
    :todo: to derive "3rd order of aggregation" from this feature, number of "1", number of consecutive "1" and etc
    """
    count = {}
    for i in range(int(x.upper)):
        count[i + 1] = None
    for i in x.list:
        if i.find("_|_") > 0 and int(i[: i.find("_|_")]) >= int(x.lower) and i.find("_|_NaN") <= 0:
            try:
                if count[int(i[: i.find("_|_")])] is None:
                    count[int(i[: i.find("_|_")])] = float(i[i.find("_|_") + 3 : :])
                else:
                    count[int(i[: i.find("_|_")])] = count.get(float(i[: i.find("_|_")]), 0) + float(i[i.find("_|_") + 3 : :])
            except ValueError:
                if count[int(i[: i.find("_|_")])] is None:
                    count[int(i[: i.find("_|_")])] = 1
                else:
                    count[int(i[: i.find("_|_")])] = count.get(float(i[: i.find("_|_")]), 0) + 1
    L = ""
    for idx, key in enumerate(count.keys()):
        if idx > 0:
            if count[key - 1] is not None and count[key] is not None:
                if count[key - 1] < count[key]:
                    L = "0" + L
                elif count[key - 1] > count[key]:
                    L = "1" + L
                elif count[key - 1] == count[key]:
                    L = "_" + L
            elif count[key - 1] is None and count[key] is not None:
                L = ">" + L
            elif count[key - 1] is not None and count[key] is None:
                L = "<" + L
            elif count[key - 1] is None and count[key] is None:
                L = "." + L
    return L


changes_hist_udf = F.udf(changes_hist, T.StringType())


# separate native and non-native aggregations into separate lists
def separate_aggregations(all_aggregations):
    native_aggregations = filter_aggregations(all_aggregations, list(all_native_aggregations.keys()))
    udf_aggregations = filter_aggregations(all_aggregations, list(all_udf_aggregations.keys()))
    # other_aggregations = filter_aggregations(all_aggregations, all_non_native_aggregations)
    return native_aggregations, udf_aggregations


# validate if selected feature is available from input dataset
def validate_features(features, df_columns):
    features_ = [feat.strip() for feat in features.split(",")]
    for f in features_:
        if f not in df_columns:
            print("{} does not exist in df".format(f))
    return features_


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


def validate_rules(input_rules):
    validation_errors = []

    # 1. Check if all names are correct
    for rule in input_rules:
        if rule.name not in ["basic", "basic_days", "since", "ratio"]:
            validation_errors.append("Invalid rule by name '{}' found.".format(rule.name))
            break

        error_message = "Rule '{}' missing all the required params".format(rule.name)

        if (rule.name == "basic" or rule == "since") and not (rule.aggregations and rule.features):
            validation_errors.append(error_message)
        elif rule.name == "basic_days" and not (rule.aggregations and rule.features and rule.dayLimitLower1 and rule.dayLimitUpper1):
            validation_errors.append(error_message)
        elif rule.name == "ratio" and not (
            rule.aggregations and rule.features and rule.dayLimitLower1 and rule.dayLimitUpper1 and rule.dayLimitLower2 and rule.dayLimitUpper2
        ):
            validation_errors.append(error_message)

    return validation_errors


def validate_agg_functions(input_rules):
    validation_errors = []

    # 1. check some aggregations function might not applicable
    for rule in input_rules:
        if rule.name not in ["basic_days"] and "changes_hist" in rule.aggregations:
            validation_errors.append("Invalid changes_hist aggregation by name '{}' found.".format(rule.name))
            break

    return validation_errors


def _create_function(name, doc=""):
    """Create a function for aggregator by name"""

    def _(col):
        sc = SparkContext._active_spark_context
        jc = getattr(sc._jvm.functions, name)(col._jc if isinstance(col, Column) else col)
        return F.Column(jc)

    return _


def reconstruct_namedtuple_rules(rules):
    namedtuple_rules = []
    for rule in rules:
        namedtuple_rules.append(AggregationSpec(rule[0], rule[1], rule[2], rule[3], rule[4], rule[5], rule[6], rule[7]))
    return namedtuple_rules


class SecondOrderAggregator(Transformer, Serialisable):
    """
    Performs aggregations on the numeric and categorical features based on the input rules.
    There are three types of supported aggregation functions:
        1. native spark - those that are supported by spark, e.g. count, min
        2. python UDF - those that can't be expressed using native spark aggregation functions, e.g. entropy

    The various rules of aggregation are:

    1. Aggregate the records per id (all parameters mandatory)
    'count, max, min', 'feature'
    (see unit test test_second_order_feature_aggregator.py/test_aggregation_rule_length_2)

    2. Aggregate the records per id over the specified number of days between application and feature dates:
    (all parameters mandatory)
    'count, max, min', 'feature', 'application_date', 'feature_date', 1, 30

    3. Aggregate the records per id as ratio: (all parameters mandatory)
    (application_date - feature_date) between 1 and 30
    / (application_date - feature_date) between 31 and 60
    'max, min, count', 'feature', 'application_date', 'feature_date', 1, 30, 31, 60

    4. Aggregate the records for days between application and feature dates per id:
    'since', 'feature', 'application_date', 'feature_date', 'max, min, std', 'custom_rule'
    ('custom_rule' can be an empty string.
    If empty, default filter of 'days_between' >= 0 will be applied.)

    The output is a dataset with id column plus columns obtained after applying the aggregation rules.

    """

    @keyword_only
    def __init__(
        self,
        rules=None,
        methods=None,
        udfs=None,
        idCol=None,
        applicationDateCol=None,
        featureDateCol=None,
        applicationDateFormat=None,
        featureDateFormat=None,
    ):
        super(SecondOrderAggregator, self).__init__()
        self.methods = Param(
            self,
            "methods",
            "Dictionary of the aggregation methods " "that can be performed on input dataset",
        )
        self.udfs = Param(
            self,
            "udfs",
            "Dictionary of the udf aggregation methods" "that can be performed on input dataset",
        )
        self.rules = Param(self, "rules", "Aggregation rules to be applied")
        self.idCol = Param(self, "idCol", "ID column name of the dataset")
        self.applicationDateCol = Param(self, "applicationDateCol", "application date column name of the dataset")
        self.featureDateCol = Param(self, "featureDateCol", "feature date column name of the dataset")
        self.applicationDateFormat = Param(
            self,
            "applicationDateFormat",
            "date format of application date column in the dataset",
        )
        self.featureDateFormat = Param(
            self,
            "featureDateFormat",
            "date format of feature date column in the dataset",
        )

        # these are all the native spark aggregation functions
        self._setDefault(methods=all_native_aggregations)
        self._setDefault(udfs=all_udf_aggregations)
        self._setDefault(featureDateCol="day")
        self._setDefault(applicationDateFormat="yyyy-MM-dd")
        self._setDefault(featureDateFormat="yyyy-MM-dd")
        self._set(**self._input_kwargs)

    def getMethods(self):
        return self.getOrDefault(self.methods)

    def setRules(self, value):
        return self._set(rules=value)

    def getRules(self):
        return self.getOrDefault(self.rules)

    def setIdCol(self, value):
        return self._set(idCol=value)

    def getIdCol(self):
        return self.getOrDefault(self.idCol)

    def setApplicationDateCol(self, value):
        return self._set(applicationDateCol=value)

    def getApplicationDateCol(self):
        return self.getOrDefault(self.applicationDateCol)

    def setFeatureDateCol(self, value):
        return self._set(featureDateCol=value)

    def getFeatureDateCol(self):
        return self.getOrDefault(self.featureDateCol)

    def setApplicationDateFormat(self, value):
        return self._set(applicationDateFormat=value)

    def getApplicationDateFormat(self):
        return self.getOrDefault(self.applicationDateFormat)

    def setFeatureDateFormat(self, value):
        return self._set(featureDateFormat=value)

    def getFeatureDateFormat(self):
        return self.getOrDefault(self.featureDateFormat)

    def getUdfs(self):
        return self.getOrDefault(self.udfs)

    def _transform(self, dataset):
        """
        :param dataset: input dataset to be transformed
        :return: aggregated dataset after applying all the rules
        """

        # create the function since functions are not JSON serializable and hence cannot be saved
        methods = self.getMethods()
        udfs = self.getUdfs()
        functions = {}
        for k, v in methods.items():
            functions[k] = _create_function(v)

        for k, v in udfs.items():
            functions[k] = eval(v)

        # reconstruct rules after reading a saved pipeline
        rules = self.getRules()
        if len(rules) > 0 and isinstance(rules[0], list):
            rules = reconstruct_namedtuple_rules(rules)

        id_col = self.getIdCol()
        application_date_col = self.getApplicationDateCol()
        feature_date_col = self.getFeatureDateCol()
        application_date_format = self.getApplicationDateFormat()
        feature_date_format = self.getFeatureDateFormat()

        # validate rules
        validation_errors = validate_rules(rules)
        if validation_errors:
            raise ValueError("Following validation errors found: {}".format(validation_errors))

        # valiedate agg functions
        validation_errors = validate_agg_functions(rules)
        if validation_errors:
            raise ValueError("Following validation errors found: {}".format(validation_errors))

        bd_agg_exprs = []
        b_agg_exprs = []
        r_agg_exprs = []
        s_agg_exprs = []
        since_filter_condition = F.col(DAYS_BETWEEN) >= 0

        for idx, rule in enumerate(rules):
            agg_col_names = validate_features(rule.features, dataset.columns)

            # calculate days_between application_date and feature_date once,
            # only if at least one of the input rules need it
            if rule.name != "basic":
                if DAYS_BETWEEN not in dataset.columns:
                    dataset = dataset.withColumn(
                        DAYS_BETWEEN,
                        F.datediff(
                            F.to_date(application_date_col, application_date_format),
                            F.to_date(feature_date_col, feature_date_format),
                        ),
                    )

            # Aggregate the records for numeric_feature per id
            if rule.name == "basic":
                print("Applying basic aggregation: " + ", ".join(rule))

                # filter Spark native aggregations aggregations
                native_aggregations, udf_aggregations = separate_aggregations(rule.aggregations)

                b_agg_exprs = (
                    b_agg_exprs
                    + generate_aggregation_expressions(
                        functions,
                        native_aggregations,
                        agg_col_names,
                        None,
                        None,
                        None,
                    )
                    + generate_udf_aggregation_expressions(
                        functions,
                        udf_aggregations,
                        agg_col_names,
                        None,
                        None,
                        feature_date_col,
                        None,
                    )
                )

            elif rule.name == "basic_days":
                print("Applying AggregationSpec for aggregation over past days: " + ", ".join([str(item) for item in rule]))

                # filter Spark native aggregations aggregations
                native_aggregations, udf_aggregations = separate_aggregations(rule.aggregations)

                bd_agg_exprs = (
                    bd_agg_exprs
                    + generate_aggregation_expressions(
                        functions,
                        native_aggregations,
                        agg_col_names,
                        str(rule.dayLimitLower1),
                        str(rule.dayLimitUpper1),
                        DAYS_BETWEEN,
                    )
                    + generate_udf_aggregation_expressions(
                        functions,
                        udf_aggregations,
                        agg_col_names,
                        str(rule.dayLimitLower1),
                        str(rule.dayLimitUpper1),
                        feature_date_col,
                        DAYS_BETWEEN,
                    )
                )

            # Aggregate the records for numeric_feature per id as ratio of:
            # days_between between day_limit_lower1 and day_limit_upper1 / days_between between day_limit_lower2 and day_limit_upper2
            elif rule.name == "ratio":
                print("Applying ratio aggregation: " + ", ".join([str(item) for item in rule]))

                native_aggregations, udf_aggregations = separate_aggregations(rule.aggregations)

                r_agg_exprs = (
                    r_agg_exprs
                    + generate_ratio_aggregation_expressions(
                        functions,
                        native_aggregations,
                        agg_col_names,
                        str(rule.dayLimitLower1),
                        str(rule.dayLimitUpper1),
                        str(rule.dayLimitLower2),
                        str(rule.dayLimitUpper2),
                        DAYS_BETWEEN,
                    )
                    + generate_ratio_udf_aggregation_expressions(
                        functions,
                        udf_aggregations,
                        agg_col_names,
                        str(rule.dayLimitLower1),
                        str(rule.dayLimitUpper1),
                        str(rule.dayLimitLower2),
                        str(rule.dayLimitUpper2),
                        feature_date_col,
                        DAYS_BETWEEN,
                    )
                )

            # Aggregate the records for numeric_feature per id as ratio of:
            # days_between between day_limit_lower1 and day_limit_upper1 / days_between between day_limit_lower2 and day_limit_upper2
            elif rule.name == "since":
                print("Applying time since aggregation: " + ", ".join([str(item) for item in rule]))

                native_aggregations, udf_aggregations = separate_aggregations(rule.aggregations)

                since_filter_condition = F.col(DAYS_BETWEEN) >= 0
                conds_ = []
                for cond in rule.filterCondition.split(","):
                    if len(cond.strip()) > 0:
                        conds_.append(F.expr(cond.strip()))
                    else:
                        conds_.append(None)

                s_agg_exprs = (
                    s_agg_exprs
                    + generate_aggregation_expressions(functions, native_aggregations, agg_col_names, None, None, DAYS_BETWEEN, conds_)
                    + generate_udf_aggregation_expressions(functions, udf_aggregations, agg_col_names, None, None, feature_date_col, DAYS_BETWEEN, conds_)
                )

        agg_col_names = [ii for i in [validate_features(rule.features, dataset.columns) for rule in rules] for ii in i]

        df_list = []
        if {"basic_days", "ratio"}.intersection([rule.name for rule in rules]):
            max_ = max(
                [r.dayLimitUpper1 for r in rules if len(str(r.dayLimitLower1).strip()) > 0]
                + [r.dayLimitUpper2 for r in rules if len(str(r.dayLimitUpper2).strip()) > 0]
            )
            min_ = min(
                [r.dayLimitLower1 for r in rules if len(str(r.dayLimitLower1).strip()) > 0]
                + [r.dayLimitLower2 for r in rules if len(str(r.dayLimitLower2).strip()) > 0]
            )
            df_agg_bd_r = (
                dataset.select(*[id_col, feature_date_col, DAYS_BETWEEN] + list(set(agg_col_names)))
                .filter((F.col(DAYS_BETWEEN) >= min_) & (F.col(DAYS_BETWEEN) <= max_))
                .groupby(id_col)
                .agg(*bd_agg_exprs + r_agg_exprs)
            )
            df_list.append(df_agg_bd_r)

        if {"basic"}.intersection([rule.name for rule in rules]):
            df_agg_b = dataset.select(*[id_col, feature_date_col] + list(set(agg_col_names))).groupby(id_col).agg(*b_agg_exprs)
            df_list.append(df_agg_b)

        if {"since"}.intersection([rule.name for rule in rules]):
            df_agg_s = (
                dataset.where(since_filter_condition)
                .select(*[id_col, feature_date_col, DAYS_BETWEEN] + list(set(agg_col_names)))
                .groupby(id_col)
                .agg(*s_agg_exprs)
            )
            df_agg_s = df_agg_s.select(
                *[id_col]
                + [
                    F.col(c).alias(
                        "SINCE" + c[len(c) - c[::-1].find("_") - 1 : :] + "_" + c[0 : len(c) - c[::-1].find("_") - 1] + "_GEO_D"
                        if c[len(c) - c[::-1].find("_") - 1 : :]
                        in [
                            "_COUNT",
                            "_MEAN",
                            "_MIN",
                            "_MAX",
                            "_STD",
                            "_SUM",
                            "_ENTROPY",
                        ]
                        else "SINCE"
                        + c[len(c) - c[::-1].find("_", c[::-1].find("_") + 1) - 1 : :]
                        + "_"
                        + c[0 : len(c) - c[::-1].find("_", c[::-1].find("_") + 1) - 1]
                        + "_GEO_D"
                    )
                    for c in df_agg_s.columns
                    if c not in [id_col]
                ]
            )
            df_list.append(df_agg_s)

        if len(df_list) == 1:
            df_agg = df_list[0]
        else:
            df_agg = df_list[0]
            for df_next in df_list[1:]:
                df_agg = df_agg.join(df_next, on=id_col, how="outer")

        return df_agg
