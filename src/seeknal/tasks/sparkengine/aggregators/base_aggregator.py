from typing import List, Optional

from pydantic import BaseModel, model_validator

from ..transformers import ColumnRenamed, Transformer


class ColByExpression(BaseModel):
    """
    Describe aggregator ColByExpression

    Attributes:
        newName (str): new column name
        expression (str): SQL expression to create the new column
    """

    newName: str
    expression: str


class RenamedCols(BaseModel):
    """
    Describe aggregator RenamedCols

    Attributes:
        name (str): current column
        newName (str): new column name
    """

    name: str
    newName: str


class AggregatorFunction(BaseModel):
    """
    Describe Aggregator function

    Attributes:
        class_name (str): aggregator class name
        params (dict): parameters correspond to the aggregator function
    """

    class_name: str = ""
    params: dict = {}


class Aggregator(BaseModel):
    """
    Describe Aggregator

    Attributes:
        group_by_cols (List[str]): columns for grouping statement
        pivot_key_col (str, optional): if defined, select which column as pivot key column
        pivot_value_cols (List[str], optional): if defined, select columns that contains value for pivot
        col_by_expression (List[ColByExpression], optional): apply expression for result column
        renamed_cols (List[RenamedCols], optional): rename result column
        aggregators (List[AggregatorFunction]): aggregator functions that use for aggregate the data
        pre_stages (List[Transformer], optional): add pre steps before aggregation
        post_stages (List[Transformer], optional): add post steps after aggregation
    """

    group_by_cols: List[str]
    pivot_key_col: Optional[str] = None
    pivot_value_cols: Optional[List[str]] = None
    col_by_expression: Optional[List[ColByExpression]] = None
    renamed_cols: Optional[List[RenamedCols]] = None
    aggregators: List[AggregatorFunction]
    pre_stages: Optional[List[Transformer]] = None
    post_stages: Optional[List[Transformer]] = None


class LastNDaysAggregator(Aggregator):
    """
    Define Last N days aggregator. This a time-based aggregation, given a last date in dataset,
    it will take last number of N days (define as window) from last date then apply aggregation. For example::

        # Given source data:
        # DataFrame:
        #        msisdn   date_id  hit
        # 0  WZNIqKAlbO  20190308    6
        # 1  yewHtPM2Kk  20190323    0
        # 2  uW4Tek3u6u  20190308    3
        # 3  9hXOz2wy5L  20190327    7
        # 4  V85AkDy9kM  20190301    3
        # 5  6nx2OlqL55  20190322    2
        # 6  dTfQFsI2cf  20190321    1
        # 7  UBTuNfFc2w  20190322    2
        # 8  lzIwdkCDEQ  20190331    5
        # 9  SL1EIPN56d  20190305    6
        # Schema:
        # |- msisdn: STRING
        # |- date_id: STRING
        # |- hit: INT

        # lets create a 10 days `hit` window aggregation from latest date that seen in the data (20190331)
        ten_days_aggr = G.LastNDaysAggregator(group_by_cols=["msisdn"], window=10, date_col="date_id", aggregators=[G.FunctionAggregator(inputCol="hit", outputCol="hit_sum_1_10", accumulatorFunction="sum")])
        create_window_aggr_data = CreateDatasetTask(name="create_window_aggr_data")
            .add_input(hive_table="databridge_small_celcom.db_charging_hourly")
            .set_date_col(date_col="date_id")
            .add_stage(aggregator=ten_days_aggr)

        create_window_aggr_data.evaluate()

        # Result:
        # DataFrame:
        #        msisdn   date_id  hit_sum_1_10
        # 0  JX9AnJD5eK  20190331            15
        # 1  Mc8g5sVhFk  20190331             3
        # 2  fSdW61wh5x  20190331             4
        # 3  m2rxkV2PcC  20190331             8
        # 4  ncY4KodfF6  20190331             7
        # 5  gSNJbbh4ho  20190331             8
        # 6  W3zyJANGM5  20190331            16
        # 7  3ovKjn144p  20190331             1
        # 8  aTGaJ2Radm  20190331             6
        # 9  32v6nD3Caw  20190331             5
        # Schema:
        # |- msisdn: STRING
        # |- date_id: STRING
        # |- hit_sum_1_10: BIGINT


    Attributes:
        group_by_cols (List[str]): columns for grouping statement
        pivot_key_col (str, optional): if defined, select which column as pivot key column
        pivot_value_cols (List[str], optional): if defined, select columns that contains value for pivot
        col_by_expression (List[ColByExpression], optional): apply expression for result column
        renamed_cols (List[RenamedCols], optional): rename result column
        aggregators (List[AggregatorFunction]): aggregator functions that use for aggregate the data
        window (int): number of days to lookback from latest date in the data
        date_col (str): column that contains date
        date_pattern (str): date pattern that describe the date value
    """

    window: int
    date_col: str
    date_pattern: str

    def model_post_init(self, __context):
        self.pre_stages = [
            Transformer(
                class_name="tech.mta.seeknal.transformers.AddColumnByExpr",
                params={"expression": "1", "outputCol": "_seeknal_dummy"},
            ),
            Transformer(
                class_name="tech.mta.seeknal.transformers.AddWindowFunction",
                params={
                    "inputCol": self.date_col,
                    "windowFunction": "max",
                    "partitionCols": ["_seeknal_dummy"],
                    "outputCol": "_seeknal_max_date",
                },
            ),
            Transformer(
                class_name="tech.mta.seeknal.transformers.AddColumnByExpr",
                params={
                    "expression": "date_format(date_sub(to_timestamp(_seeknal_max_date, '{0}'), {1}), '{0}')".format(
                        self.date_pattern, self.window
                    ),
                    "outputCol": "_seeknal_date",
                },
            ),
            Transformer(
                class_name="tech.mta.seeknal.transformers.FilterByExpr",
                params={"expression": "{} >= _seeknal_date".format(self.date_col)},
            ),
        ]
        if self.group_by_cols is None:
            raise ValueError("group_by_cols must be setted")
        new_group_by_cols = self.group_by_cols + ["_seeknal_max_date"]
        object.__setattr__(self, 'group_by_cols', new_group_by_cols)
        new_post_stages = [ColumnRenamed(inputCol="_seeknal_max_date", outputCol=self.date_col)]
        object.__setattr__(self, 'post_stages', new_post_stages)
