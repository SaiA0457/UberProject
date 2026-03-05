from pyspark import pipelines as dp

# Step 1: Create the target streaming table
# dim_passenger

@dp.view
def dim_passanger__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
    df = df.dropDuplicates(["passenger_id"])

    return df

dp.create_streaming_table("dim_passanger")

# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_passanger",
  source = "dim_passanger__view",  # e.g., ingested from JDBC into bronze
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = "1"
)

# dim_driver

@dp.view
def dim_driver__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
    df = df.dropDuplicates(["driver_id"])

    return df

dp.create_streaming_table("dim_driver")

# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver__view",  # e.g., ingested from JDBC into bronze
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = "1"
)


# dim_vehicle

@dp.view
def dim_vehicle__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("vehicle_id", "vehicle_model", "vehicle_color", "license_plate", "vehicle_type_id", "vehicle_make_id", "vehicle_type", "vehicle_make")
    df = df.dropDuplicates(["vehicle_id"])

    return df

dp.create_streaming_table("dim_vehicle")


# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle__view",  # e.g., ingested from JDBC into bronze
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = "1"
)

# dim_payment_method

@dp.view
def dim_payment_method__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
    df = df.dropDuplicates(["payment_method_id"])

    return df

dp.create_streaming_table("dim_payment_method")


# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_payment_method",
  source = "dim_payment_method__view",  # e.g., ingested from JDBC into bronze
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = "1"
)

# dim_bookings

@dp.view
def dim_bookings__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_latitude","pickup_longitude","pickup_location_id")
    df = df.dropDuplicates(["ride_id"])

    return df

dp.create_streaming_table("dim_bookings")


# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_bookings",
  source = "dim_bookings__view",  # e.g., ingested from JDBC into bronze
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = "1"
)


# dim_cancellation_reason

@dp.view
def dim_cancellation_reason__view():
    df = spark.readStream.table("silver_obt")
    df = df.select("cancellation_reason_id", "cancellation_reason")
    df = df.dropDuplicates(["cancellation_reason_id"])

    return df

dp.create_streaming_table("dim_cancellation_reason")


# Step 2: Once Flow — Load initial snapshot of full RDBMS table

dp.create_auto_cdc_flow(
  target = "dim_cancellation_reason",
  source = "dim_cancellation_reason__view",  # e.g., ingested from JDBC into bronze
  keys = ["cancellation_reason_id"],
  sequence_by = "cancellation_reason_id",
  stored_as_scd_type = "1"
)

# Dim Location
@dp.table
def dim_location_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("pickup_city_id","pickup_city","city_updated_at","region","state",)
    df = df.dropDuplicates(['pickup_city_id', "city_updated_at"])
    return df

dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "city_updated_at",
  stored_as_scd_type = 2,
)


# Fact Table
@dp.view
def fact_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","total_fare","tip_amount","rating","base_rate","per_mile","per_minute")
    return df

dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)