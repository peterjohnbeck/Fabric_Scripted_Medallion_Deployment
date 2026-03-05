# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a3440210-0db4-4bca-b1f1-e09591b04aee",
# META       "default_lakehouse_name": "SilverLakehouse",
# META       "default_lakehouse_workspace_id": "528aaa1c-d26e-4f40-913d-b07ca8001485",
# META       "known_lakehouses": [
# META         {
# META           "id": "a3440210-0db4-4bca-b1f1-e09591b04aee"
# META         },
# META         {
# META           "id": "c2c1ae53-1ac4-4abc-a866-733182f6f14b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Silver Layer — Dimensional Model Transformation
# # Reads raw Bronze tables from **BronzeLakehouse** and writes a cleansed,
# conformed dimensional model into **SilverLakehouse** (the default lakehouse).
# # **Output tables (write order):**
# 1. `dim_date` — full calendar 2022-01-01 → 2025-12-31
# 2. `dim_territory`
# 3. `dim_currency`
# 4. `dim_product_category`
# 5. `dim_product`
# 6. `dim_employee`
# 7. `dim_customer`  *(includes customer_segment derived from revenue)*
# 8. `fact_sales`    *(USD-converted amounts, all FK surrogate keys)*
# # **Prerequisites:**
# - Bronze_LoadData notebook must have been run successfully
# - SilverLakehouse must be attached as the default lakehouse

# CELL ********************

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

bronze = _make_lh_reader("BronzeLakehouse")

# Confirm Bronze tables are accessible
print("Checking Bronze tables...")
for t in [
    "raw_product_categories", "raw_products", "raw_territories",
    "raw_employees", "raw_currencies", "raw_customers",
    "raw_exchange_rates", "raw_sales_orders", "raw_sales_order_lines",
]:
    n = bronze(t).count()
    print(f"  {t:<30}  {n:,} rows")
print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── dim_date  (2022-01-01 → 2025-12-31, 1461 rows) ───────────────────────────

from datetime import date, timedelta

start_date = date(2022, 1, 1)
end_date   = date(2025, 12, 31)
days       = (end_date - start_date).days + 1

MONTH_NAMES = ["January","February","March","April","May","June",
               "July","August","September","October","November","December"]
MONTH_SHORT = ["Jan","Feb","Mar","Apr","May","Jun",
               "Jul","Aug","Sep","Oct","Nov","Dec"]
DAY_NAMES   = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]

rows = []
for i in range(days):
    d     = start_date + timedelta(days=i)
    yr    = d.year
    mo    = d.month
    dy    = d.day
    dow   = d.isoweekday() % 7   # 0=Sunday, 6=Saturday
    q     = (mo - 1) // 3 + 1
    iso_w = d.isocalendar()[1]
    fy    = yr                    # fiscal year = calendar year
    fq    = q
    fm    = mo
    rows.append((
        yr * 10000 + mo * 100 + dy,          # date_key
        d.isoformat(),                        # full_date
        yr, q, mo,
        MONTH_NAMES[mo - 1],                  # month_name
        MONTH_SHORT[mo - 1],                  # month_name_short
        iso_w,                                # week_of_year
        dow,                                  # day_of_week  (0=Sun)
        DAY_NAMES[dow],                       # day_name
        dy,                                   # day_of_month
        (d - date(yr, 1, 1)).days + 1,        # day_of_year
        dow in (0, 6),                        # is_weekend
        dow not in (0, 6),                    # is_weekday
        fy, fq, fm,
        f"FY{fy}-Q{fq}",                      # fiscal_period_name
        f"Q{q} {yr}",                         # quarter_name
        f"{yr}-{mo:02d}",                     # year_month
    ))

dim_date_schema = [
    "date_key","full_date","year","quarter","month","month_name","month_name_short",
    "week_of_year","day_of_week","day_name","day_of_month","day_of_year",
    "is_weekend","is_weekday","fiscal_year","fiscal_quarter","fiscal_month",
    "fiscal_period_name","quarter_name","year_month",
]

df_dim_date = spark.createDataFrame(rows, schema=dim_date_schema)
df_dim_date.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_date")
print(f"dim_date  —  {df_dim_date.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, row_number, col, lit, when, concat_ws, round as spark_round

# ── dim_territory ─────────────────────────────────────────────────────────────

df_raw_terr = bronze("raw_territories")

df_dim_terr = (
    df_raw_terr
    .withColumn("territory_key", row_number().over(Window.orderBy("territory_id")))
    .withColumn("is_domestic", col("territory_type") == "Domestic")
    .withColumn("dw_created_at", F.current_timestamp())
    .select(
        "territory_key","territory_id","territory_name","territory_type",
        "region_code","is_domestic","manager_name","dw_created_at",
    )
)

df_dim_terr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_territory")
print(f"dim_territory  —  {df_dim_terr.count()} rows")

# ── dim_currency ──────────────────────────────────────────────────────────────

df_raw_curr = bronze("raw_currencies")

df_dim_curr = (
    df_raw_curr
    .withColumn("currency_key", row_number().over(Window.orderBy("currency_code")))
    .withColumn("dw_created_at", F.current_timestamp())
    .select(
        "currency_key","currency_code","currency_name","currency_symbol",
        "is_base_currency","decimal_places","dw_created_at",
    )
)

df_dim_curr.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_currency")
print(f"dim_currency  —  {df_dim_curr.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── dim_product_category ──────────────────────────────────────────────────────

df_raw_cat = bronze("raw_product_categories")

df_dim_cat = (
    df_raw_cat
    .withColumn("category_key", row_number().over(Window.orderBy("category_id")))
    .withColumn("dw_created_at", F.current_timestamp())
    .select(
        "category_key","category_id","category_name","category_desc",
        "is_capital_equip","dw_created_at",
    )
)

df_dim_cat.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_product_category")
print(f"dim_product_category  —  {df_dim_cat.count()} rows")

# ── dim_product ───────────────────────────────────────────────────────────────

df_raw_prod = bronze("raw_products")

df_dim_prod = (
    df_raw_prod
    .join(df_dim_cat.select("category_id","category_key","category_name","is_capital_equip"),
          on="category_id", how="left")
    .withColumn("product_key", row_number().over(Window.orderBy("product_id")))
    .withColumn("gross_margin_pct",
        spark_round(
            (col("list_price_usd") - col("standard_cost_usd")) / col("list_price_usd"), 4
        )
    )
    .withColumn("dw_created_at", F.current_timestamp())
    .withColumn("dw_updated_at", F.current_timestamp())
    .select(
        "product_key","product_id","category_key","category_id","category_name",
        "product_code","product_name","description","list_price_usd","standard_cost_usd",
        "gross_margin_pct","weight_kg","lead_time_days","is_capital_equip","is_active",
        "dw_created_at","dw_updated_at",
    )
)

df_dim_prod.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_product")
print(f"dim_product  —  {df_dim_prod.count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── dim_employee ──────────────────────────────────────────────────────────────

df_raw_emp = bronze("raw_employees")

df_dim_emp = (
    df_raw_emp
    .join(df_dim_terr.select("territory_id","territory_key"), on="territory_id", how="left")
    .withColumn("employee_key", row_number().over(Window.orderBy("employee_id")))
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    .withColumn(
        "tenure_years",
        spark_round(F.datediff(F.current_date(), F.to_date(col("hire_date"))) / 365.25, 2)
    )
    .withColumn("dw_created_at", F.current_timestamp())
    .withColumn("dw_updated_at", F.current_timestamp())
    .select(
        "employee_key","employee_id","full_name","email","territory_key",
        "hire_date","tenure_years","is_active","dw_created_at","dw_updated_at",
    )
)

df_dim_emp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_employee")
print(f"dim_employee  —  {df_dim_emp.count()} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── dim_customer ──────────────────────────────────────────────────────────────
# Customer segment (Enterprise / Mid-Market / SMB) is derived from each customer's
# share of total historical revenue. We build a preliminary fact revenue rollup
# first, then join it back when writing dim_customer.

df_raw_cust  = bronze("raw_customers")
df_raw_ord   = bronze("raw_sales_orders")
df_raw_lines = bronze("raw_sales_order_lines")
df_raw_fx    = bronze("raw_exchange_rates")

# Build a revenue-by-customer rollup in USD (approximate, using order-level FX)
df_ord_lines = (
    df_raw_lines
    .join(df_raw_ord.select("order_id","customer_id","order_date","currency_code"),
          on="order_id", how="left")
    .join(
        df_raw_fx.select("rate_date","to_currency","exchange_rate")
                 .withColumnRenamed("to_currency","currency_code")
                 .withColumnRenamed("rate_date","order_date"),
        on=["order_date","currency_code"], how="left"
    )
    .withColumn(
        "extended_price_usd",
        when(
            (col("currency_code") == "USD") | col("exchange_rate").isNull(),
            col("extended_price")
        ).otherwise(
            spark_round(col("extended_price") / col("exchange_rate"), 2)
        )
    )
)

df_cust_rev = (
    df_ord_lines
    .groupBy("customer_id")
    .agg(F.sum("extended_price_usd").alias("total_revenue_usd"))
)

# Compute percentile thresholds
from pyspark.sql.functions import percentile_approx

thresholds = df_cust_rev.select(
    percentile_approx("total_revenue_usd", 0.90).alias("p90"),
    percentile_approx("total_revenue_usd", 0.60).alias("p60"),
).collect()[0]

p90 = float(thresholds["p90"])
p60 = float(thresholds["p60"])

df_cust_seg = (
    df_cust_rev
    .withColumn(
        "customer_segment",
        when(col("total_revenue_usd") >= p90, "Enterprise")
        .when(col("total_revenue_usd") >= p60, "Mid-Market")
        .otherwise("SMB")
    )
    .select("customer_id","customer_segment")
)

# Build dim_customer
df_dim_cust = (
    df_raw_cust
    .join(df_dim_terr.select("territory_id","territory_key","is_domestic"),
          on="territory_id", how="left")
    .join(df_cust_seg, on="customer_id", how="left")
    .withColumn("customer_key", row_number().over(Window.orderBy("customer_id")))
    .withColumn(
        "contact_name", concat_ws(" ", col("contact_first"), col("contact_last"))
    )
    .withColumn(
        "full_address",
        concat_ws(", ",
            col("address_line1"), col("city"),
            col("state_province"), col("postal_code"), col("country_code")
        )
    )
    .withColumn("dw_created_at", F.current_timestamp())
    .withColumn("dw_updated_at", F.current_timestamp())
    .select(
        "customer_key","customer_id","company_name","contact_name","contact_email",
        "phone","full_address","city","state_province","postal_code",
        "country_code","country_name","territory_key","is_domestic",
        "customer_segment","is_active","customer_since","dw_created_at","dw_updated_at",
    )
)

df_dim_cust.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("dim_customer")
print(f"dim_customer  —  {df_dim_cust.count():,} rows")
print(f"  Segment thresholds:  p90 (Enterprise) = ${p90:,.0f}  |  p60 (Mid-Market) = ${p60:,.0f}")
display(df_dim_cust.groupBy("customer_segment").count().orderBy("customer_segment"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── fact_sales ────────────────────────────────────────────────────────────────
# Grain: 1 row per sales order line item
# All FK columns are surrogate keys joining to dim tables in SilverLakehouse.

# Re-read dims we need for key lookups
df_dim_date_keys = spark.table("dim_date").select("full_date","date_key")
df_prod_keys     = spark.table("dim_product").select("product_id","product_key","category_key")
df_emp_keys      = spark.table("dim_employee").select("employee_id","employee_key")
df_cust_keys     = spark.table("dim_customer").select("customer_id","customer_key","territory_key","is_domestic")
df_terr_keys     = spark.table("dim_territory").select("territory_id","territory_key")
df_curr_keys     = spark.table("dim_currency").select("currency_code","currency_key")

# Alias territory keys to avoid collision: order territory vs customer territory
df_ord_terr_keys = df_terr_keys.withColumnRenamed("territory_key","order_territory_key")

df_fact = (
    df_raw_lines
    # Join order header
    .join(df_raw_ord, on="order_id", how="inner")

    # Join FX rates (USD = no conversion needed)
    .join(
        df_raw_fx
          .withColumnRenamed("to_currency","currency_code")
          .withColumnRenamed("rate_date","order_date")
          .select("order_date","currency_code","exchange_rate"),
        on=["order_date","currency_code"], how="left"
    )

    # Resolve exchange rate: USD orders get rate = 1.0
    .withColumn(
        "fx",
        when((col("currency_code") == "USD") | col("exchange_rate").isNull(), F.lit(1.0))
        .otherwise(col("exchange_rate"))
    )

    # USD-converted measures
    .withColumn("unit_price_usd",     spark_round(col("unit_price") / col("fx"), 2))
    .withColumn("extended_price_usd", spark_round(col("extended_price") / col("fx"), 2))
    .withColumn("standard_cost_total_usd",
        spark_round(col("unit_cost_usd") * col("quantity"), 2))
    .withColumn("gross_profit_usd",
        spark_round(col("extended_price_usd") - col("standard_cost_total_usd"), 2))
    .withColumn(
        "gross_margin_pct",
        when(col("extended_price_usd") != 0,
            spark_round(col("gross_profit_usd") / col("extended_price_usd"), 4)
        ).otherwise(F.lit(0.0))
    )

    # Surrogate key joins
    .join(df_dim_date_keys.withColumnRenamed("full_date","order_date")
                          .withColumnRenamed("date_key","date_key"),
          on="order_date", how="left")
    .join(
        df_dim_date_keys.withColumnRenamed("full_date","shipped_date")
                        .withColumnRenamed("date_key","shipped_date_key"),
        on="shipped_date", how="left"
    )
    .join(df_prod_keys,     on="product_id",   how="left")
    .join(df_emp_keys,      on="employee_id",  how="left")
    .join(df_cust_keys,     on="customer_id",  how="left")
    .join(df_ord_terr_keys.withColumnRenamed("territory_id","territory_id"),
          on="territory_id", how="left")
    .join(df_curr_keys,     on="currency_code", how="left")

    # Partition helper
    .withColumn("year", F.year(F.to_date(col("order_date"))))

    # Surrogate key for fact table
    .withColumn("sales_key",
        row_number().over(Window.orderBy("order_id","line_number")))

    .select(
        "sales_key",
        # Degenerate dimensions
        col("order_id"), col("line_id"), col("order_number"), col("line_number"),
        # Date keys
        col("date_key"),
        col("shipped_date_key"),
        # FK surrogate keys
        col("customer_key"),
        col("product_key"),
        col("category_key"),
        col("employee_key"),
        col("order_territory_key").alias("territory_key"),
        col("currency_key"),
        # Measures — local currency
        col("quantity"),
        col("unit_price").alias("unit_price_local"),
        col("discount_pct"),
        col("extended_price").alias("extended_price_local"),
        # Measures — USD
        col("unit_price_usd"),
        col("extended_price_usd"),
        col("standard_cost_total_usd").alias("standard_cost_usd"),
        col("gross_profit_usd"),
        col("gross_margin_pct"),
        # Flags
        col("is_domestic"),
        col("status").alias("order_status"),
        # Partition column
        col("year"),
    )
)

(
    df_fact.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("year")
    .saveAsTable("fact_sales")
)

print(f"fact_sales  —  {spark.table('fact_sales').count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ── Validation ────────────────────────────────────────────────────────────────

print("=" * 60)
print("VALIDATION")
print("=" * 60)

silver_tables = [
    ("dim_date",             1461),
    ("dim_territory",          13),
    ("dim_currency",           10),
    ("dim_product_category",   10),
    ("dim_product",           500),
    ("dim_employee",           28),
    ("dim_customer",          500),
    ("fact_sales",          None),   # ~75,000
]

for tbl, exp in silver_tables:
    n = spark.table(tbl).count()
    status = "OK" if (exp is None or n >= exp * 0.98) else "WARN"
    label  = f"~{exp:,}" if exp else "~75,000"
    print(f"  {status}  {tbl:<25}  {n:>8,}  (expected {label})")

# Referential integrity: nulls in FK columns of fact_sales
print()
print("Null FK checks in fact_sales:")
fact = spark.table("fact_sales")
for fk_col in ["date_key","customer_key","product_key","employee_key","territory_key","currency_key"]:
    null_count = fact.filter(col(fk_col).isNull()).count()
    status = "OK" if null_count == 0 else f"WARN — {null_count:,} nulls"
    print(f"  {fk_col:<25}  {status}")

# Margin sanity check
print()
print("Gross margin distribution in fact_sales:")
display(
    fact.select("gross_margin_pct")
        .summary("min","25%","50%","75%","max")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
