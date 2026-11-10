"""Gold mart 2: behaviour of subscribers (member) versus walk-up riders (casual)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TOP_CORRIDORS_PER_TIER = 10


def build_user_kpis(trips_df: DataFrame) -> DataFrame:
    """Build trip KPIs with one row per rider tier and bike type."""
    return trips_df.groupBy("member_casual", "rideable_type").agg(
        F.count(F.lit(1)).alias("total_trips"),
        F.avg("trip_duration_min").alias("avg_duration_min"),
        F.percentile_approx("trip_duration_min", 0.5).alias("median_duration_min"),
        F.avg("speed_kmh").alias("avg_speed_kmh"),
        F.avg("distance_km").alias("avg_distance_km"),
    )


def build_top_corridors(trips_df: DataFrame, stations_df: DataFrame) -> DataFrame:
    """Build the most travelled station pairs of each rider tier.

    Silver trips carry station ids only, so names come from the catalog. A station
    missing from the catalog falls back to its id so its trips are not lost.
    """
    names = (
        stations_df.select("station_id", "station_name")
        .filter(F.col("station_id").isNotNull())
        .dropDuplicates(["station_id"])
    )
    start_names = names.select(
        F.col("station_id").alias("start_station_id"),
        F.col("station_name").alias("start_station_name"),
    )
    end_names = names.select(
        F.col("station_id").alias("end_station_id"),
        F.col("station_name").alias("end_station_name"),
    )

    corridors = (
        trips_df.filter(
            F.col("start_station_id").isNotNull() & F.col("end_station_id").isNotNull()
        )
        .join(F.broadcast(start_names), on="start_station_id", how="left")
        .join(F.broadcast(end_names), on="end_station_id", how="left")
        .withColumn(
            "start_station_name", F.coalesce(F.col("start_station_name"), F.col("start_station_id"))
        )
        .withColumn(
            "end_station_name", F.coalesce(F.col("end_station_name"), F.col("end_station_id"))
        )
        .groupBy(
            "member_casual",
            "start_station_id",
            "start_station_name",
            "end_station_id",
            "end_station_name",
        )
        .agg(F.count(F.lit(1)).alias("trips"))
    )

    window = Window.partitionBy("member_casual").orderBy(
        F.col("trips").desc(), F.col("start_station_id"), F.col("end_station_id")
    )
    return (
        corridors.withColumn("corridor_rank", F.row_number().over(window))
        .filter(F.col("corridor_rank") <= TOP_CORRIDORS_PER_TIER)
    )
