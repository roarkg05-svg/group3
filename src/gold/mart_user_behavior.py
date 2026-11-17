"""Gold mart 2: behaviour of subscribers (member) versus walk-up riders (casual)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

TOP_CORRIDORS_PER_TIER = 10


def build_user_kpis(trips_df: DataFrame) -> DataFrame:
    """Build trip KPIs with one row per rider tier and bike type using explicit Spark SQL."""
    spark = trips_df.sparkSession
    trips_df.createOrReplaceTempView("silver_trips_view")
    duration_col = (
        "trip_duration_min"
        if "trip_duration_min" in trips_df.columns
        else "duration_minutes"
    )
    return spark.sql(
        f"""
        SELECT
            member_casual,
            rideable_type,
            COUNT(1) AS total_trips,
            AVG({duration_col}) AS avg_duration_min,
            percentile_approx({duration_col}, 0.5) AS median_duration_min,
            AVG(speed_kmh) AS avg_speed_kmh,
            AVG(distance_km) AS avg_distance_km
        FROM silver_trips_view
        GROUP BY member_casual, rideable_type
        """
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

    trips_base = trips_df
    for col_name in ("start_station_name", "end_station_name"):
        if col_name in trips_base.columns:
            trips_base = trips_base.drop(col_name)

    corridors = (
        trips_base.filter(
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
