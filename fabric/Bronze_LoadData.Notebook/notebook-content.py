# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c2c1ae53-1ac4-4abc-a866-733182f6f14b",
# META       "default_lakehouse_name": "BronzeLakehouse",
# META       "default_lakehouse_workspace_id": "528aaa1c-d26e-4f40-913d-b07ca8001485",
# META       "known_lakehouses": [
# META         {
# META           "id": "c2c1ae53-1ac4-4abc-a866-733182f6f14b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Layer — Load Raw Data
# Downloads 9 CSV files from the GitHub repository into **BronzeLakehouse Files/**
# (organised into topic subfolders), then reads those staged files into Delta tables.
# **Prerequisites:**
# - BronzeLakehouse must be attached as the default lakehouse
# - The GitHub repository must be public so the notebook can reach raw.githubusercontent.com
# **Run order:** Run all cells top-to-bottom. Approximate runtime: 3–7 minutes.

# CELL ********************

# Base URL for raw CSV files in the GitHub repository.
# NOTE: The repository must be public when this notebook is executed.
GITHUB_BASE_URL = (
    "https://raw.githubusercontent.com"
    "/peterjohnbeck/Fabric_Scripted_Medallion_Deployment"
    "/main/data/bronze"
)

# Mapping of table name → Files/ subfolder
SUBFOLDERS = {
    "raw_product_categories": "Products",
    "raw_products":           "Products",
    "raw_customers":          "Customers",
    "raw_sales_orders":       "Sales",
    "raw_sales_order_lines":  "Sales",
    "raw_territories":        "Reference",
    "raw_employees":          "Reference",
    "raw_currencies":         "Reference",
    "raw_exchange_rates":     "Reference",
}

# Load order: reference tables first, transactions last
LOAD_ORDER = [
    "raw_product_categories",
    "raw_products",
    "raw_territories",
    "raw_employees",
    "raw_currencies",
    "raw_customers",
    "raw_exchange_rates",
    "raw_sales_orders",
    "raw_sales_order_lines",
]

print(f"Source  : {GITHUB_BASE_URL}")
print(f"Tables  : {len(SUBFOLDERS)}")
print(f"Folders : Products, Customers, Sales, Reference")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Phase 1 — Download CSVs from GitHub into BronzeLakehouse Files/
#
# The default lakehouse Files section is mounted at /lakehouse/default/Files/
# and is writable via standard Python file I/O from the Spark driver.

import os
import requests

FILES_ROOT = "/lakehouse/default/Files"

print("Phase 1: Downloading CSVs to BronzeLakehouse Files/")
print("=" * 60)

for table, subfolder in SUBFOLDERS.items():
    dest_dir  = f"{FILES_ROOT}/{subfolder}"
    dest_file = f"{dest_dir}/{table}.csv"
    os.makedirs(dest_dir, exist_ok=True)

    url = f"{GITHUB_BASE_URL}/{table}.csv"
    print(f"  {table:<30} → Files/{subfolder}/  ...", end="  ")

    r = requests.get(url, timeout=120)
    r.raise_for_status()

    with open(dest_file, "wb") as fh:
        fh.write(r.content)

    size_kb = len(r.content) / 1024
    print(f"{size_kb:,.0f} KB")

print()
print("All CSVs staged to BronzeLakehouse Files/.")
print()
print("Subfolder summary:")
for folder in ["Products", "Customers", "Sales", "Reference"]:
    files = [t for t, sf in SUBFOLDERS.items() if sf == folder]
    print(f"  Files/{folder}/  →  {', '.join(files)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Phase 2 — Read staged CSVs from Files/ and write as Delta tables.
#
# Spark reads CSV files from the default lakehouse Files/ section using the
# relative "Files/..." path (Fabric resolves this for the default lakehouse).
# inferSchema=true handles all column types automatically.

print("Phase 2: Loading Delta tables from Files/")
print("=" * 60)

for table in LOAD_ORDER:
    subfolder = SUBFOLDERS[table]
    csv_path  = f"Files/{subfolder}/{table}.csv"
    print(f"  Loading {table} ...", end="  ")

    df = (
        spark.read
             .format("csv")
             .option("header", "true")
             .option("inferSchema", "true")
             .load(csv_path)
    )

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(table)
    )

    row_count = spark.table(table).count()
    print(f"{row_count:,} rows written")

print()
print("All tables loaded successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validation — print row counts for all 9 tables.

print("=" * 60)
print("VALIDATION: Row Counts")
print("=" * 60)

expected = {
    "raw_product_categories":  10,
    "raw_products":           500,
    "raw_territories":         13,
    "raw_employees":           28,
    "raw_currencies":          10,
    "raw_customers":          500,
    "raw_exchange_rates":    9864,
    "raw_sales_orders":     50000,
    "raw_sales_order_lines": None,   # ~75,000
}

all_ok = True
for table, exp in expected.items():
    actual = spark.table(table).count()
    status = "OK" if (exp is None or actual >= exp * 0.98) else "WARN"
    if status == "WARN":
        all_ok = False
    label = f"~{exp:,}" if exp else "~75,000"
    print(f"  {status}  {table:<30}  {actual:>8,}  (expected {label})")

print()
if all_ok:
    print("All checks passed.")
else:
    print("WARNING: Some row counts are below expected minimums.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quick spot-check: sample rows from key tables.

print("── raw_products sample ──────────────────────────────")
display(spark.table("raw_products").limit(5))

print("── raw_customers sample (US and international) ──────")
display(spark.table("raw_customers").limit(5))

print("── raw_sales_order_lines sample ─────────────────────")
display(spark.table("raw_sales_order_lines").limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
