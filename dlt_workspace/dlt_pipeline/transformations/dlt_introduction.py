import dlt
from pyspark.sql.functions import *

#Reading the parameters
_order_status = spark.conf.get("custom.orderStatus","NA")
#Creating orders streaming bronze table
@dlt.table(
    table_properties={"quality": "bronze"},
    comment = "orders bronze table"
)

def orders_bronze():
    df = spark.readStream.table("dev.bronze.orders_raw")
    return df

#Creating orders streaming bronze table sourced using AUTOLOADER
@dlt.table(
    table_properties={"quality": "bronze"},
    comment = "orders Autoloader",
    name = "orders_autoloader_bronze"
)

def func():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.schemaHints","o_orderkey bigint ,o_custkey bigint,o_orderstatus string,o_totalprice decimal(18,2),o_orderdate date,o_orderpriority string ,o_clerk string,o_shippriority int,o_comment string")
        .option("cloudFiles.schemaLocation","/Volumes/dev/bronze/landing/autoloader/schemas/")
        .option("cloudFiles.format","CSV")
        .option("pathGlobFilter","*.csv")
        .option("cloudFiles.schemaEvolutionMode","none")
        .load("/Volumes/dev/bronze/landing/files/")
    )
    return df

#Create blank Streaming Tables 
dlt.create_streaming_table("orders_union_bronze")

#Append Flow
@dlt.append_flow(
    target = "orders_union_bronze"
)
def order_delta_append():
    df = spark.readStream.table("LIVE.orders_bronze")
    return df

#Append Flow
@dlt.append_flow(
    target = "orders_union_bronze"
)
def order_autoloader_append():
    df = spark.readStream.table("LIVE.orders_autoloader_bronze")
    return df

#Creating customer materialized view
@dlt.table(
    table_properties={"quality": "bronze"},
    comment = "customers bronze table",
    name = "customer_bronze"
)

def customer_bronze():
    df = spark.read.table("dev.bronze.customers_raw")
    return df

#creating view to join orders table and customer table
@dlt.view(
    comment = "Joined table"
)

def joined_vw():
    df_c = spark.read.table("LIVE.customer_bronze")
    df_o = spark.read.table("LIVE.orders_union_bronze")
    df = df_o.join(df_c,how="left_outer", on=df_o.o_custkey == df_c.c_custkey)
    return df

#create MV to add a new column
@dlt.table(
    table_properties={"quality": "silver"},
    comment = "joined silver table",
    name = "joined_silver"
)

def joined_silver():
    df = spark.read.table("LIVE.joined_vw").withColumn("__insert_date",current_timestamp())
    return df

#Aggregate based on c_mktsegment and find the count of order (c_orderkey)
@dlt.table(
    table_properties = {"quality": "gold"},
    comment = "orders aggregated table"
)

def orders_agg_gold():
    df = spark.read.table("LIVE.joined_silver")
    df_final = df.groupBy("c_mktsegment").agg(count("o_orderkey").alias("count_orders"))
    return df_final

for _status in _order_status.split(","):
    @dlt.table(
    table_properties = {"quality": "gold"},
    comment = "orders aggregated table",
    name = f"orders_agg_{_status}_gold"
    )

    def func():
        df = spark.read.table("LIVE.joined_silver")
        df_final = df.where(f"o_orderstatus = '{_status}'").groupBy("c_mktsegment").agg(count("o_orderkey").alias("count_orders"))
        return df_final
