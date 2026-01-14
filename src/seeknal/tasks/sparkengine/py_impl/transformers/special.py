"""Special transformers requiring UDFs."""

import math
from pyspark.sql import DataFrame, functions as F, types as T
from ..base import BaseTransformerPySpark


class AddEntropy(BaseTransformerPySpark):
    """Add entropy calculation column.

    Args:
        input_col: Input column with array of probabilities
        output_col: Output column name
    """

    def __init__(self, input_col: str, output_col: str, **kwargs):
        super().__init__(**kwargs, input_col=input_col, output_col=output_col)
        self.input_col = input_col
        self.output_col = output_col

    def _entropy(self, probs):
        """Calculate Shannon entropy."""
        if not probs:
            return 0.0
        return -sum(p * math.log2(p) for p in probs if p > 0)

    def transform(self, df: DataFrame) -> DataFrame:
        """Add entropy column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with entropy column
        """
        # Create UDF
        entropy_udf = F.udf(self._entropy, T.FloatType())
        return df.withColumn(self.output_col, entropy_udf(F.col(self.input_col)))


class AddLatLongDistance(BaseTransformerPySpark):
    """Add haversine distance column between two lat/long points.

    Args:
        lat1_col: First latitude column
        lon1_col: First longitude column
        lat2_col: Second latitude column
        lon2_col: Second longitude column
        output_col: Output column name
    """

    def __init__(
        self,
        lat1_col: str,
        lon1_col: str,
        lat2_col: str,
        lon2_col: str,
        output_col: str,
        **kwargs
    ):
        super().__init__(**kwargs, lat1_col=lat1_col, lon1_col=lon1_col,
                         lat2_col=lat2_col, lon2_col=lon2_col, output_col=output_col)
        self.lat1_col = lat1_col
        self.lon1_col = lon1_col
        self.lat2_col = lat2_col
        self.lon2_col = lon2_col
        self.output_col = output_col

    def _haversine_distance(self, lat1, lon1, lat2, lon2):
        """Calculate haversine distance in kilometers."""
        R = 6371  # Earth radius in km

        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))

        return R * c

    def transform(self, df: DataFrame) -> DataFrame:
        """Add distance column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with distance column
        """
        # Create UDF
        distance_udf = F.udf(self._haversine_distance, T.FloatType())
        return df.withColumn(
            self.output_col,
            distance_udf(
                F.col(self.lat1_col),
                F.col(self.lon1_col),
                F.col(self.lat2_col),
                F.col(self.lon2_col)
            )
        )
