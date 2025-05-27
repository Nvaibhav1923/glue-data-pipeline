import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, when, to_timestamp, to_date, lower

# Setup logger for visible output in Glue logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("Starting Glue Job")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

base_path = "s3://bi-s3bucket-123/dms-sql-data/dbo/"
paths = {
    "date_dim": base_path + "date_dim/",
    "order_item_options": base_path + "order_item_options/",
    "order_items": base_path + "order_items/"
}

def load_and_rename(path, col_names):
    df = spark.read.option("header", "false").csv(path)
    df = df.toDF(*[name.strip().upper() for name in col_names])
    return df

# Define schemas manually (based on CSV structure)
order_items_cols = [
    "APP_NAME", "RESTAURANT_ID", "CREATION_TIME_UTC", "ORDER_ID", "USER_ID",
    "PRINTED_CARD_NUMBER", "IS_LOYALTY", "CURRENCY", "LINEITEM_ID",
    "ITEM_CATEGORY", "ITEM_NAME", "ITEM_PRICE", "ITEM_QUANTITY"
]

order_item_options_cols = [
    "ORDER_ID", "LINEITEM_ID", "OPTION_GROUP_NAME",
    "OPTION_NAME", "OPTION_PRICE", "OPTION_QUANTITY"
]

date_dim_cols = [
    "DATE_KEY", "DAY_OF_WEEK", "WEEK", "MONTH", "YEAR",
    "IS_WEEKEND", "IS_HOLIDAY", "HOLIDAY_NAME"
]

# Load data with correct column names
oi = load_and_rename(paths["order_items"], order_items_cols)
oi_options = load_and_rename(paths["order_item_options"], order_item_options_cols)
dd = load_and_rename(paths["date_dim"], date_dim_cols)

# Clean and transform order_items
df_cleaned_oi = oi.select(
    when(col("APP_NAME").isNull(), "NULL").otherwise(col("APP_NAME")).alias("app_name"),
    when(col("RESTAURANT_ID").isNull(), "NULL").otherwise(col("RESTAURANT_ID")).alias("restaurant_id"),
    to_timestamp(col("CREATION_TIME_UTC"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("creation_time_utc"),
    when(col("ORDER_ID").isNull(), "NULL").otherwise(col("ORDER_ID")).alias("order_id"),
    when(col("USER_ID").isNull(), "NULL").otherwise(col("USER_ID")).alias("user_id"),
    when(col("PRINTED_CARD_NUMBER").isNull(), None).otherwise(col("PRINTED_CARD_NUMBER").cast("int")).alias("printed_card_number"),
    when(col("IS_LOYALTY").isNull(), None)
        .when(lower(col("IS_LOYALTY").cast("string")).isin("true", "1"), True)
        .otherwise(False).alias("is_loyalty"),
    when(col("CURRENCY").isNull(), "NULL").otherwise(col("CURRENCY")).alias("currency"),
    when(col("LINEITEM_ID").isNull(), "NULL").otherwise(col("LINEITEM_ID")).alias("lineitem_id"),
    when(col("ITEM_CATEGORY").isNull(), "NULL").otherwise(col("ITEM_CATEGORY")).alias("item_category"),
    when(col("ITEM_NAME").isNull(), "NULL").otherwise(col("ITEM_NAME")).alias("item_name"),
    when(col("ITEM_PRICE").isNull(), None).otherwise(col("ITEM_PRICE").cast("double")).alias("item_price"),
    when(col("ITEM_QUANTITY").isNull(), None).otherwise(col("ITEM_QUANTITY").cast("int")).alias("item_quantity")
)

# Clean and transform order_item_options
df_order_item_options_cleaned = oi_options.select(
    when(col("ORDER_ID").isNull(), "NULL").otherwise(col("ORDER_ID")).alias("order_id"),
    when(col("LINEITEM_ID").isNull(), "NULL").otherwise(col("LINEITEM_ID")).alias("lineitem_id"),
    when(col("OPTION_GROUP_NAME").isNull(), "NULL").otherwise(col("OPTION_GROUP_NAME")).alias("option_group_name"),
    when(col("OPTION_NAME").isNull(), "NULL").otherwise(col("OPTION_NAME")).alias("option_name"),
    when(col("OPTION_PRICE").isNull(), None).otherwise(col("OPTION_PRICE").cast("float")).alias("option_price"),
    when(col("OPTION_QUANTITY").isNull(), None).otherwise(col("OPTION_QUANTITY").cast("int")).alias("option_quantity")
)

# Clean and transform date_dim
df_date_dim_cleaned = dd.select(
    to_date(when(col("DATE_KEY").isNull(), None).otherwise(col("DATE_KEY")), "dd-MM-yyyy").alias("date_key"),
    when(col("DAY_OF_WEEK").isNull(), "NULL").otherwise(col("DAY_OF_WEEK")).alias("day_of_week"),
    when(col("WEEK").isNull(), None).otherwise(col("WEEK").cast("int")).alias("week"),
    when(col("MONTH").isNull(), None).otherwise(col("MONTH").cast("int")).alias("month"),
    when(col("YEAR").isNull(), None).otherwise(col("YEAR").cast("int")).alias("year"),
    when(col("IS_WEEKEND").isNull(), None)
        .when(lower(col("IS_WEEKEND").cast("string")).isin("true", "1"), True)
        .otherwise(False).alias("is_weekend"),
    when(col("IS_HOLIDAY").isNull(), None)
        .when(lower(col("IS_HOLIDAY").cast("string")).isin("true", "1"), True)
        .otherwise(False).alias("is_holiday"),
    when(col("HOLIDAY_NAME").isNull(), "NULL").otherwise(col("HOLIDAY_NAME")).alias("holiday_name")
)

# Join order_items with options
df_joined_oi_options = df_cleaned_oi.join(
    df_order_item_options_cleaned,
    on=["order_id", "lineitem_id"],
    how="left"
)

# Add order_date column
df_joined_with_date = df_joined_oi_options.withColumn(
    "order_date", to_date("creation_time_utc")
)

# Join with date_dim for enriched date attributes
df_final = df_joined_with_date.join(
    df_date_dim_cleaned.select("date_key", "is_weekend", "is_holiday", "holiday_name", "day_of_week"),
    df_joined_with_date["order_date"] == df_date_dim_cleaned["date_key"],
    how="left"
).drop("date_key")

# Show preview of result (this appears in Glue logs)
df_final.show(5, truncate=False)


output_path = "s3://transformed-s3-global-patners/transformed_data/"

df_final.write.mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

logger.info("Glue Job Completed Successfully — data written to S3.")
