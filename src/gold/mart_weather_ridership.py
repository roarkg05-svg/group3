"""Gold mart 3: how weather conditions relate to trip volume, distance and speed.

Grain: one row per temperature bin and rain bin.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

UNKNOWN_BIN = "Unknown"


def _hourly_weather(weather_df: DataFrame) -> DataFrame:
    """Return exactly one typed weather observation per hour.

    Bronze keeps CSV values as strings, so numeric fields are cast here. try_cast
    turns a malformed value into null, because Spark 4 ANSI casts would abort the
    run. Keeping a single row per hour stops the trip join from multiplying trips.
    """
    typed = (
        weather_df.withColumn(
            "weather_hour",
            F.date_trunc("hour", F.expr("try_to_timestamp(trim(weather_timestamp))")),
        )
        .withColumn("temp_c", F.expr("try_cast(temp_c AS double)"))
        .withColumn("precipitation_mm", F.expr("try_cast(precipitation_mm AS double)"))
        .filter(F.col("weather_hour").isNotNull())
    )
    latest_first = Window.partitionBy("weather_hour").orderBy(
        F.col("weather_timestamp").desc(),
        F.col("_ingestion_timestamp").desc(),
        F.col("_source_file").desc(),
    )
    return (
        typed.withColumn("_rank", F.row_number().over(latest_first))
        .filter(F.col("_rank") == 1)
        .select("weather_hour", "temp_c", "precipitation_mm")
    )


def _temperature_bin(col: F.Column) -> F.Column:
    return (
        F.when(col.isNull(), F.lit(UNKNOWN_BIN))
        .when(col < 0, F.lit("Freezing (<0°C)"))
        .when(col < 10, F.lit("Cold (0-10°C)"))
        .when(col <= 20, F.lit("Mild (10-20°C)"))
        .otherwise(F.lit("Warm (>20°C)"))
    )


def _rain_bin(col: F.Column) -> F.Column:
    return (
        F.when(col.isNull(), F.lit(UNKNOWN_BIN))
        .when(col <= 0, F.lit("No Rain (0.0mm)"))
        .when(col <= 5, F.lit("Light Rain (>0-5.0mm)"))
        .otherwise(F.lit("Heavy Rain (>5.0mm)"))
    )


def join_trips_with_weather(trips_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """Attach the binned weather of the start hour to each trip that has a matching hour."""
    trips_by_hour = trips_df.withColumn("trip_hour", F.date_trunc("hour", F.col("started_at")))
    weather = _hourly_weather(weather_df)
    return (
        trips_by_hour.join(
            F.broadcast(weather),
            trips_by_hour["trip_hour"] == weather["weather_hour"],
            how="inner",
        )
        .withColumn("temperature_bin", _temperature_bin(F.col("temp_c")))
        .withColumn("rain_bin", _rain_bin(F.col("precipitation_mm")))
    )


def build_weather_ridership(trips_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    """Build the weather mart from clean trips and raw hourly weather."""
    return (
        join_trips_with_weather(trips_df, weather_df)
        .groupBy("temperature_bin", "rain_bin")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.countDistinct("trip_hour").alias("hours_with_trips"),
            F.avg("distance_km").alias("avg_distance_km"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
        )
        .withColumn("avg_trips_per_hour", F.col("total_trips") / F.col("hours_with_trips"))
    )
