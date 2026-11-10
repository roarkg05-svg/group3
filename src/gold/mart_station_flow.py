"""Gold mart 1: daily station demand balance for bike rebalancing logistics.

Grain: one row per station and calendar day.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _station_catalog(stations_df: DataFrame) -> DataFrame:
    """Return one row per catalog station with the attributes the mart publishes."""
    return (
        stations_df.select(
            F.col("station_id"),
            F.col("station_name"),
            F.col("region_id"),
            F.col("capacity").cast("long").alias("capacity"),
        )
        .filter(F.col("station_id").isNotNull())
        .dropDuplicates(["station_id"])
    )


def _peak_departure_hours(trips_df: DataFrame) -> DataFrame:
    """Return the busiest departure hour of each station and day.

    row_number with the hour as tie-break keeps exactly one row per station and
    day. dense_rank would return one row per tied hour and duplicate the mart.
    """
    hourly = (
        trips_df.filter(F.col("start_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("started_at"))
        .withColumn("departure_hour", F.hour("started_at"))
        .groupBy("start_station_id", "trip_date", "departure_hour")
        .agg(F.count(F.lit(1)).alias("peak_departure_trips"))
    )
    window = Window.partitionBy("start_station_id", "trip_date").orderBy(
        F.col("peak_departure_trips").desc(), F.col("departure_hour").asc()
    )
    return (
        hourly.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .select(
            F.col("start_station_id").alias("station_id"),
            F.col("trip_date"),
            F.col("departure_hour").alias("peak_departure_hour"),
            F.col("peak_departure_trips"),
        )
    )


def build_station_flow(trips_df: DataFrame, stations_df: DataFrame) -> DataFrame:
    """Build the station balance mart from clean trips and the station catalog.

    A departure counts on the day the trip started and an arrival on the day it
    ended, so a trip crossing midnight moves stock on the correct day.

    Stations missing from the catalog are kept, flagged with
    is_catalog_station = False. Their capacity is unknown, so turnover_ratio is null.
    """
    outbound = (
        trips_df.filter(F.col("start_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("started_at"))
        .groupBy(F.col("start_station_id").alias("station_id"), "trip_date")
        .agg(F.count(F.lit(1)).alias("outbound_trips"))
    )
    inbound = (
        trips_df.filter(F.col("end_station_id").isNotNull())
        .withColumn("trip_date", F.to_date("ended_at"))
        .groupBy(F.col("end_station_id").alias("station_id"), "trip_date")
        .agg(F.count(F.lit(1)).alias("inbound_trips"))
    )

    catalog = _station_catalog(stations_df).withColumn("is_catalog_station", F.lit(True))

    return (
        outbound.join(inbound, on=["station_id", "trip_date"], how="full_outer")
        .fillna(0, subset=["outbound_trips", "inbound_trips"])
        .join(F.broadcast(catalog), on="station_id", how="left")
        .join(_peak_departure_hours(trips_df), on=["station_id", "trip_date"], how="left")
        .withColumn("is_catalog_station", F.coalesce(F.col("is_catalog_station"), F.lit(False)))
        .withColumn("net_flow", F.col("inbound_trips") - F.col("outbound_trips"))
        .withColumn(
            "turnover_ratio",
            F.when(
                F.col("capacity") > 0,
                (F.col("inbound_trips") + F.col("outbound_trips")) / F.col("capacity"),
            ),
        )
        .withColumn("year", F.year("trip_date"))
        .withColumn("month", F.month("trip_date"))
        .select(
            "station_id",
            "trip_date",
            "station_name",
            "region_id",
            "capacity",
            "is_catalog_station",
            "inbound_trips",
            "outbound_trips",
            "net_flow",
            "turnover_ratio",
            "peak_departure_hour",
            "peak_departure_trips",
            "year",
            "month",
        )
    )
