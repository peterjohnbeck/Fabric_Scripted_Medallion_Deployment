# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "96938cda-37cb-4885-8a1f-bf5f8b81df30",
# META       "default_lakehouse_name": "GoldLakehouse",
# META       "default_lakehouse_workspace_id": "528aaa1c-d26e-4f40-913d-b07ca8001485",
# META       "known_lakehouses": [
# META         {
# META           "id": "96938cda-37cb-4885-8a1f-bf5f8b81df30"
# META         },
# META         {
# META           "id": "a3440210-0db4-4bca-b1f1-e09591b04aee"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer — Business-Ready Aggregations
# # Reads the Silver dimensional model from **SilverLakehouse** and writes
# 6 business-ready aggregated tables into **GoldLakehouse** (the default lakehouse).
# # **Output tables:**
# 1. `gold_sales_by_customer`
# 2. `gold_sales_by_product`
# 3. `gold_sales_by_territory`
# 4. `gold_sales_by_month`
# 5. `gold_product_category_performance`
# 6. `gold_international_vs_domestic`
# # **Prerequisites:** Silver_Transformation notebook must have been run successfully.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, when, round as spark_round, sum as spark_sum,
    count, countDistinct, avg, min as spark_min, max as spark_max,
    lit, lag, first
)

import notebookutils

# ── Cross-lakehouse ABFS reader ───────────────────────────────────────────────
# spark_catalog only supports single-part namespace; cross-lakehouse reads via
# spark.table("OtherLakehouse.default.table") fail with REQUIRES_SINGLE_PART_NAMESPACE.
# Solution: read Delta tables directly from their OneLake ABFS path.

def _make_lh_reader(lakehouse_name):
    lh    = notebookutils.lakehouse.get(lakehouse_name)
    lh_id = lh.id
    ws_id = lh.workspaceId
    base  = (
        f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com"
        f"/{lh_id}/Tables"
    )
    def _read(table_name):
        return spark.read.format("delta").load(f"{base}/{table_name}")
    return _read

silver = _make_lh_reader("SilverLakehouse")

# ── Load Silver tables ─────────────────────────────────────────────────────────

fact       = silver("fact_sales")
dim_cust   = silver("dim_customer")
dim_prod   = silver("dim_product")
dim_cat    = silver("dim_product_category")
dim_terr   = silver("dim_territory")
dim_date   = silver("dim_date")
dim_curr   = silver("dim_currency")

print("Silver tables loaded:")
for name, df in [("fact_sales",fact),("dim_customer",dim_cust),("dim_product",dim_prod),
                 ("dim_product_category",dim_cat),("dim_territory",dim_terr),("dim_date",dim_date)]:
    print(f"  {name:<25}  {df.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Denormalized base view ─────────────────────────────────────────────────────
# Join fact with key dimension attributes once; reuse for all aggregations.

base = (
    fact
    # year is already in fact; drop it from dim_date to avoid AMBIGUOUS_REFERENCE
    .join(dim_date.select("date_key","quarter","month","year_month","quarter_name"),
          on="date_key", how="left")
    # territory_key and is_domestic are already in fact; drop from dim_cust to avoid ambiguity
    .join(dim_cust.select("customer_key","customer_id","company_name","country_code",
                          "customer_segment"),
          on="customer_key", how="left")
    # category_key is already in fact; drop from dim_prod to avoid ambiguity
    .join(dim_prod.select("product_key","product_id","product_code","product_name",
                          "list_price_usd"),
          on="product_key", how="left")
    .join(dim_cat.select("category_key","category_id","category_name","is_capital_equip"),
          on="category_key", how="left")
    .join(dim_terr.select("territory_key","territory_id","territory_name",
                          "territory_type","region_code"),
          on="territory_key", how="left")
    .cache()
)

print(f"Base view rows: {base.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_sales_by_customer ─────────────────────────────────────────────────────

REFRESHED = F.current_timestamp()

# Pivot by year: sum revenue per year
def year_revenue(df, y):
    return spark_sum(when(col("year") == y, col("extended_price_usd"))).alias(f"revenue_{y}_usd")

def year_orders(df, y):
    return countDistinct(when(col("year") == y, col("order_id"))).alias(f"orders_{y}")

df_gold_cust = (
    base
    .groupBy(
        "customer_key","customer_id","company_name","country_code",
        "is_domestic","customer_segment"
    )
    .agg(
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("standard_cost_usd").alias("total_cost_usd"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        countDistinct("order_id").alias("total_orders"),
        count("line_id").alias("total_lines"),
        spark_sum("quantity").alias("total_units"),
        spark_min(F.to_date(col("date_key").cast("string"), "yyyyMMdd")).alias("first_order_date"),
        spark_max(F.to_date(col("date_key").cast("string"), "yyyyMMdd")).alias("last_order_date"),
        year_revenue(base, 2022), year_revenue(base, 2023), year_revenue(base, 2024),
        year_orders(base, 2022),  year_orders(base, 2023),  year_orders(base, 2024),
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_order_value_usd",
        spark_round(col("total_revenue_usd") / col("total_orders"), 2)
    )
    .withColumn("avg_lines_per_order",
        spark_round(col("total_lines") / col("total_orders"), 2)
    )
    .withColumn("years_as_customer",
        spark_round(
            F.datediff(F.current_date(), col("first_order_date")) / 365.25, 2
        )
    )
    .withColumn("customer_rank",
        F.rank().over(Window.orderBy(col("total_revenue_usd").desc()))
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "customer_key","customer_id","company_name","country_code","is_domestic",
        "customer_segment","total_revenue_usd","total_cost_usd","total_gross_profit_usd",
        "gross_margin_pct","total_orders","total_lines","avg_order_value_usd",
        "avg_lines_per_order","first_order_date","last_order_date","years_as_customer",
        "revenue_2022_usd","revenue_2023_usd","revenue_2024_usd",
        "orders_2022","orders_2023","orders_2024",
        "customer_rank","refreshed_at",
    )
)

df_gold_cust.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_sales_by_customer")
print(f"gold_sales_by_customer  —  {df_gold_cust.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_sales_by_product ──────────────────────────────────────────────────────

df_gold_prod = (
    base
    .groupBy(
        "product_key","product_id","product_code","product_name",
        "category_key","category_name","is_capital_equip","list_price_usd"
    )
    .agg(
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("standard_cost_usd").alias("total_cost_usd"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        spark_sum("quantity").alias("total_units_sold"),
        countDistinct("order_id").alias("total_orders"),
        year_revenue(base, 2022), year_revenue(base, 2023), year_revenue(base, 2024),
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_selling_price_usd",
        when(col("total_units_sold") != 0,
            spark_round(col("total_revenue_usd") / col("total_units_sold"), 2)
        ).otherwise(lit(0.0))
    )
    .withColumn("price_to_list_ratio",
        when(col("list_price_usd") != 0,
            spark_round(col("avg_selling_price_usd") / col("list_price_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("product_rank",
        F.rank().over(Window.orderBy(col("total_revenue_usd").desc()))
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "product_key","product_id","product_code","product_name",
        "category_key","category_name","is_capital_equip","list_price_usd",
        "total_revenue_usd","total_cost_usd","total_gross_profit_usd","gross_margin_pct",
        "total_units_sold","total_orders","avg_selling_price_usd","price_to_list_ratio",
        "revenue_2022_usd","revenue_2023_usd","revenue_2024_usd",
        "product_rank","refreshed_at",
    )
)

df_gold_prod.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_sales_by_product")
print(f"gold_sales_by_product  —  {df_gold_prod.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_sales_by_territory ────────────────────────────────────────────────────

df_gold_terr = (
    base
    .groupBy(
        "territory_key","territory_id","territory_name",
        "territory_type","is_domestic","region_code"
    )
    .agg(
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        year_revenue(base, 2022), year_revenue(base, 2023), year_revenue(base, 2024),
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_order_value_usd",
        spark_round(col("total_revenue_usd") / col("total_orders"), 2)
    )
    .withColumn("yoy_growth_2022_2023",
        when(col("revenue_2022_usd") != 0,
            spark_round((col("revenue_2023_usd") - col("revenue_2022_usd")) / col("revenue_2022_usd"), 4)
        ).otherwise(lit(None))
    )
    .withColumn("yoy_growth_2023_2024",
        when(col("revenue_2023_usd") != 0,
            spark_round((col("revenue_2024_usd") - col("revenue_2023_usd")) / col("revenue_2023_usd"), 4)
        ).otherwise(lit(None))
    )
)

# Compute pct_of_total_revenue
total_rev = df_gold_terr.agg(spark_sum("total_revenue_usd").alias("grand_total")).collect()[0]["grand_total"]
df_gold_terr = (
    df_gold_terr
    .withColumn("pct_of_total_revenue",
        spark_round(col("total_revenue_usd") / lit(total_rev), 4)
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "territory_key","territory_id","territory_name","territory_type",
        "is_domestic","region_code","total_revenue_usd","total_gross_profit_usd",
        "gross_margin_pct","total_orders","total_customers","avg_order_value_usd",
        "revenue_2022_usd","revenue_2023_usd","revenue_2024_usd",
        "yoy_growth_2022_2023","yoy_growth_2023_2024","pct_of_total_revenue","refreshed_at",
    )
)

df_gold_terr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_sales_by_territory")
print(f"gold_sales_by_territory  —  {df_gold_terr.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_sales_by_month ────────────────────────────────────────────────────────

df_monthly_raw = (
    base
    .groupBy("year","month","year_month","quarter","quarter_name")
    .agg(
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("standard_cost_usd").alias("total_cost_usd"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        countDistinct("order_id").alias("total_orders"),
        count("line_id").alias("total_lines"),
        spark_sum("quantity").alias("total_units_sold"),
        spark_sum(when(col("is_domestic"), col("extended_price_usd"))).alias("domestic_revenue_usd"),
        spark_sum(when(~col("is_domestic"), col("extended_price_usd"))).alias("international_revenue_usd"),
    )
    .withColumn("fiscal_year", col("year"))
    .withColumn("fiscal_quarter", col("quarter"))
    .withColumn("fiscal_month", col("month"))
    .withColumn("month_name",
        F.date_format(F.to_date(F.concat_ws("-", col("year"), col("month"), lit("01")), "yyyy-M-dd"), "MMMM")
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_order_value_usd",
        spark_round(col("total_revenue_usd") / col("total_orders"), 2)
    )
    .orderBy("year","month")
)

# Window for YoY (lag 12 months) and rolling 3-month sum
w_time = Window.orderBy("year","month")

df_gold_month = (
    df_monthly_raw
    .withColumn("prior_year_revenue_usd",
        lag("total_revenue_usd", 12).over(w_time)
    )
    .withColumn("yoy_growth_pct",
        when(col("prior_year_revenue_usd").isNotNull() & (col("prior_year_revenue_usd") != 0),
            spark_round(
                (col("total_revenue_usd") - col("prior_year_revenue_usd")) / col("prior_year_revenue_usd"),
                4
            )
        ).otherwise(lit(None))
    )
    .withColumn("rolling_3m_revenue_usd",
        spark_round(
            F.sum("total_revenue_usd").over(
                w_time.rowsBetween(-2, 0)
            ), 2
        )
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "year_month","year","month","month_name","quarter","quarter_name",
        "fiscal_year","fiscal_quarter","fiscal_month",
        "total_revenue_usd","total_cost_usd","total_gross_profit_usd","gross_margin_pct",
        "total_orders","total_lines","total_units_sold","avg_order_value_usd",
        "domestic_revenue_usd","international_revenue_usd",
        "prior_year_revenue_usd","yoy_growth_pct","rolling_3m_revenue_usd",
        "refreshed_at",
    )
)

df_gold_month.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_sales_by_month")
print(f"gold_sales_by_month  —  {df_gold_month.count()} rows  (expect 36)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_product_category_performance ─────────────────────────────────────────

df_gold_cat = (
    base
    .groupBy("category_key","category_id","category_name","is_capital_equip")
    .agg(
        countDistinct("product_id").alias("product_count"),
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("quantity").alias("total_units_sold"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        countDistinct("order_id").alias("total_orders"),
        year_revenue(base, 2022), year_revenue(base, 2023), year_revenue(base, 2024),
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_selling_price_usd",
        when(col("total_units_sold") != 0,
            spark_round(col("total_revenue_usd") / col("total_units_sold"), 2)
        ).otherwise(lit(0.0))
    )
    .withColumn("yoy_growth_2022_2023",
        when(col("revenue_2022_usd") != 0,
            spark_round((col("revenue_2023_usd") - col("revenue_2022_usd")) / col("revenue_2022_usd"), 4)
        ).otherwise(lit(None))
    )
    .withColumn("yoy_growth_2023_2024",
        when(col("revenue_2023_usd") != 0,
            spark_round((col("revenue_2024_usd") - col("revenue_2023_usd")) / col("revenue_2023_usd"), 4)
        ).otherwise(lit(None))
    )
)

cat_total = df_gold_cat.agg(spark_sum("total_revenue_usd").alias("t")).collect()[0]["t"]
df_gold_cat = (
    df_gold_cat
    .withColumn("pct_of_total_revenue",
        spark_round(col("total_revenue_usd") / lit(cat_total), 4)
    )
    .withColumn("category_rank",
        F.rank().over(Window.orderBy(col("total_revenue_usd").desc()))
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "category_key","category_id","category_name","is_capital_equip","product_count",
        "total_revenue_usd","total_units_sold","total_gross_profit_usd","gross_margin_pct",
        "avg_selling_price_usd","total_orders","pct_of_total_revenue",
        "revenue_2022_usd","revenue_2023_usd","revenue_2024_usd",
        "yoy_growth_2022_2023","yoy_growth_2023_2024","category_rank","refreshed_at",
    )
)

df_gold_cat.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_product_category_performance")
print(f"gold_product_category_performance  —  {df_gold_cat.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── gold_international_vs_domestic ─────────────────────────────────────────────

# Find top country by revenue in each segment
df_top_country = (
    base
    .groupBy("is_domestic","country_code")
    .agg(spark_sum("extended_price_usd").alias("country_rev"))
    .withColumn("rn",
        F.row_number().over(
            Window.partitionBy("is_domestic").orderBy(col("country_rev").desc())
        )
    )
    .filter(col("rn") == 1)
    .select(
        col("is_domestic"),
        col("country_code").alias("top_country_by_revenue"),
    )
)

df_gold_geo = (
    base
    .groupBy("is_domestic")
    .agg(
        spark_sum("extended_price_usd").alias("total_revenue_usd"),
        spark_sum("gross_profit_usd").alias("total_gross_profit_usd"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("total_customers"),
        spark_sum(when(col("year") == 2022, col("extended_price_usd"))).alias("revenue_2022_usd"),
        spark_sum(when(col("year") == 2023, col("extended_price_usd"))).alias("revenue_2023_usd"),
        spark_sum(when(col("year") == 2024, col("extended_price_usd"))).alias("revenue_2024_usd"),
    )
    .withColumn("segment",
        when(col("is_domestic"), lit("Domestic")).otherwise(lit("International"))
    )
    .withColumn("gross_margin_pct",
        when(col("total_revenue_usd") != 0,
            spark_round(col("total_gross_profit_usd") / col("total_revenue_usd"), 4)
        ).otherwise(lit(0.0))
    )
    .withColumn("avg_order_value_usd",
        spark_round(col("total_revenue_usd") / col("total_orders"), 2)
    )
    .withColumn("yoy_growth_2022_2023",
        when(col("revenue_2022_usd") != 0,
            spark_round((col("revenue_2023_usd") - col("revenue_2022_usd")) / col("revenue_2022_usd"), 4)
        ).otherwise(lit(None))
    )
    .withColumn("yoy_growth_2023_2024",
        when(col("revenue_2023_usd") != 0,
            spark_round((col("revenue_2024_usd") - col("revenue_2023_usd")) / col("revenue_2023_usd"), 4)
        ).otherwise(lit(None))
    )
    .join(df_top_country, on="is_domestic", how="left")
)

geo_total = df_gold_geo.agg(spark_sum("total_revenue_usd").alias("t")).collect()[0]["t"]
df_gold_geo = (
    df_gold_geo
    .withColumn("pct_of_total_revenue",
        spark_round(col("total_revenue_usd") / lit(geo_total), 4)
    )
    .withColumn("refreshed_at", REFRESHED)
    .select(
        "segment","total_revenue_usd","total_gross_profit_usd","gross_margin_pct",
        "total_orders","total_customers","avg_order_value_usd","pct_of_total_revenue",
        "revenue_2022_usd","revenue_2023_usd","revenue_2024_usd",
        "yoy_growth_2022_2023","yoy_growth_2023_2024","top_country_by_revenue","refreshed_at",
    )
)

df_gold_geo.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("gold_international_vs_domestic")
print(f"gold_international_vs_domestic  —  {df_gold_geo.count()} rows  (expect 2)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Final Validation ───────────────────────────────────────────────────────────

print("=" * 65)
print("VALIDATION: Gold Layer")
print("=" * 65)

gold_checks = [
    ("gold_sales_by_customer",            500,  None),
    ("gold_sales_by_product",             500,  None),
    ("gold_sales_by_territory",            13,  None),
    ("gold_sales_by_month",                36,  36  ),
    ("gold_product_category_performance",  10,  10  ),
    ("gold_international_vs_domestic",      2,  2   ),
]

for tbl, exp_min, exp_exact in gold_checks:
    n = spark.table(tbl).count()
    if exp_exact is not None:
        status = "OK" if n == exp_exact else f"WARN (expected exactly {exp_exact})"
    else:
        status = "OK" if n >= exp_min * 0.98 else f"WARN (expected ~{exp_min})"
    print(f"  {tbl:<42}  {n:>5}  {status}")

# Spot check: gold_sales_by_month row count and YoY
print()
print("Month coverage check (expect 36 months: Jan 2022 – Dec 2024):")
display(spark.table("gold_sales_by_month").select(
    "year_month","total_revenue_usd","yoy_growth_pct"
).orderBy("year_month"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
