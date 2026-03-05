# Fabric Scripted Medallion Deployment

A complete Microsoft Fabric medallion architecture for a fictional machine tools manufacturer, built entirely through AI-assisted development and deployed via GitHub Git integration. Covers data generation, Bronze/Silver/Gold lakehouses, a DirectLake semantic model, and a three-page Power BI report — all as plain text files in a Git repository.

---

## Architecture Overview

```
data/bronze/          ← Generated CSVs (committed to repo)
      │
      ▼
Bronze_LoadData       ← Spark notebook: loads CSVs into BronzeLakehouse Delta tables
      │
      ▼
Silver_Transformation ← Spark notebook: builds star schema in SilverLakehouse
      │
      ▼
Gold_Aggregation      ← Spark notebook: builds summary tables in GoldLakehouse
      │
      ▼
Silver_DirectLake_Semantic_Model  ← DirectLake model on SilverLakehouse (7 DAX measures)
      │
      ▼
Silver_Sales_Report   ← 3-page Power BI report
```

### Lakehouses

| Layer | Name | Contents |
|---|---|---|
| Bronze | `BronzeLakehouse` | 9 raw Delta tables loaded from CSV |
| Silver | `SilverLakehouse` | 8 star-schema tables (4 dims + fact_sales + dim_date + more) |
| Gold | `GoldLakehouse` | 6 pre-aggregated summary tables |

### Silver Star Schema Tables

| Table | Rows (approx) | Description |
|---|---|---|
| `dim_date` | 1,461 | Calendar table, 4 years |
| `dim_territory` | 13 | Sales territories |
| `dim_currency` | 10 | Currencies with exchange rates |
| `dim_product_category` | 10 | Product categories |
| `dim_product` | ~500 | Products with cost and pricing |
| `dim_employee` | 28 | Sales employees |
| `dim_customer` | 500 | B2B customers |
| `fact_sales` | ~75,000 | Order lines with revenue in USD |

### Gold Summary Tables

- `gold_sales_by_customer`
- `gold_sales_by_product`
- `gold_sales_by_territory`
- `gold_sales_by_month`
- `gold_product_category_performance`
- `gold_international_vs_domestic`

---

## Repository Structure

```
Fabric_Scripted_Medallion_Deployment/
├── data/
│   └── bronze/               ← Generated CSV source files
│       ├── raw_customers.csv
│       ├── raw_products.csv
│       ├── raw_sales_orders.csv
│       └── ...
├── fabric/                   ← Git-synced to Fabric workspace
│   ├── BronzeLakehouse.Lakehouse/
│   ├── SilverLakehouse.Lakehouse/
│   ├── GoldLakehouse.Lakehouse/
│   ├── Bronze_LoadData.Notebook/
│   ├── Silver_Transformation.Notebook/
│   ├── Gold_Aggregation.Notebook/
│   ├── Silver_DirectLake_Semantic_Model.SemanticModel/
│   └── Silver_Sales_Report.Report/
└── scripts/
    └── generate_bronze_data.py   ← Local data generation script
```

Each artifact in `fabric/` follows the Fabric Git integration format: a `.platform` file (metadata + logicalId) plus the artifact content files (notebook `.py`, TMDL files, report JSON, etc.).

---

## How to Deploy

### Prerequisites

- A Microsoft Fabric workspace with Git integration enabled
- Python with `pandas` and `numpy` installed (for data generation)
- This repo must be **public** when the Bronze notebook runs (raw CSV URLs)

### Step 1 — Connect the Fabric Workspace to This Repo

1. In your Fabric workspace, go to **Workspace Settings → Git integration**
2. Connect to this GitHub repository, branch `main`, folder `/fabric`
3. Sync — the lakehouses, notebooks, semantic model, and report will all appear

### Step 2 — Generate and Commit the Source Data

Run the data generation script locally:

```bash
python scripts/generate_bronze_data.py
```

This produces nine CSVs in `data/bronze/`. Commit and push them:

```bash
git add data/bronze/
git commit -m "Add generated bronze CSV data"
git push origin main --force
```

> **Note:** `--force` is required because the remote has a single clean commit with no shared history.

### Step 3 — Run the Notebooks in Order

For each notebook, open it in Fabric, attach the correct lakehouse, then run:

| Notebook | Attach Lakehouse | Reads From | Writes To |
|---|---|---|---|
| `Bronze_LoadData` | `BronzeLakehouse` | `data/bronze/*.csv` via GitHub raw URLs | `BronzeLakehouse` Delta tables |
| `Silver_Transformation` | `SilverLakehouse` | `BronzeLakehouse.default.*` | `SilverLakehouse` Delta tables |
| `Gold_Aggregation` | `GoldLakehouse` | `SilverLakehouse.default.*` | `GoldLakehouse` Delta tables |

> Cross-lakehouse reads require the source lakehouse to be added to the notebook's workspace resources in the Fabric UI.

### Step 4 — Verify the Semantic Model and Report

1. Open `Silver_DirectLake_Semantic_Model` — confirm it connects to `SilverLakehouse`
2. Open `Silver_Sales_Report` — the three pages should display data:
   - **Sales Overview** — 4 KPI cards, revenue trend, territory bar chart, year slicer, customer segment donut
   - **Product Performance** — category and top-15 product revenue charts, product detail table
   - **Customer Analysis** — segment and country revenue charts, customer detail table

---

## Pushing Future Changes

Because the remote has a clean single-commit history, always force-push:

```bash
git push origin main --force
```

Fabric will detect the new commit and prompt you to sync in the workspace.

---

## Data Generation Details

The script `scripts/generate_bronze_data.py` uses `seed=42` for fully reproducible output:

- ~500 products across 10 categories
- 500 B2B customers across multiple territories
- 50,000 sales orders over 3 years
- ~75,000 order lines
- 13 territories (US domestic + international)
- Daily exchange rates for 10 currencies

---

## Notes on the Fabric Artifact Format

- Each artifact has a stable `logicalId` in its `.platform` file — Fabric uses this to match repo files to workspace items across syncs
- Notebook files use Fabric-specific comment blocks (`# META`, `# CELL`) to define cell types and kernel metadata
- The semantic model uses TMDL (Tabular Model Definition Language) — one `.tmdl` file per table plus `relationships.tmdl`
- Report visuals are defined in `report.json` where each visual's `config` is an escaped JSON string