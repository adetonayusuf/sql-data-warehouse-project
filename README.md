# 🏗️ Data Warehouse & Analytics Project

![SQL Server](https://img.shields.io/badge/SQL%20Server-CC2927?style=for-the-badge&logo=microsoft-sql-server&logoColor=white)
![T-SQL](https://img.shields.io/badge/T--SQL-005C84?style=for-the-badge&logo=database&logoColor=white)
![ETL](https://img.shields.io/badge/ETL%20Pipeline-FF6B35?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Medallion](https://img.shields.io/badge/Medallion%20Architecture-FFD700?style=for-the-badge&logo=databricks&logoColor=black)
![Notion](https://img.shields.io/badge/Planned%20with%20Notion-000000?style=for-the-badge&logo=notion&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

> **A complete, end-to-end Data Warehouse and Analytics project built with SQL Server, covering data engineering, exploratory analysis, and advanced SQL analytics from a single source of truth.**

---

## 📌 Project Overview

Welcome to the **Data Warehouse and Analytics Project** repository!

This project demonstrates a full data lifecycle from raw data ingestion to advanced business analytics, across three distinct phases:

| Phase | Domain | What It Covers |
|-------|--------|----------------|
| **1. Data Engineering** | Data Warehouse Build | Medallion architecture, ETL pipeline, data modelling |
| **2. Exploratory Analysis** | EDA | Data profiling, KPIs, segmentation, trends |
| **3. Advanced Analytics** | SQL Data Analysis | Window functions, CTEs, report views, YoY analysis |

Designed as a **portfolio project**, it showcases industry best practices in data engineering and analytics, planned end-to-end using **Notion**.

---

## 🏛️ Data Architecture

The warehouse is built on the **Medallion Architecture**  a layered design pattern that ensures data quality, traceability, and analytical readiness at every stage.


![Data Architecture](https://github.com/adetonayusuf/sql-data-warehouse-project/blob/main/doc/Data-Architecture.png)


| Layer | Name | Description |
|-------|------|-------------|
| 🥉 | **Bronze** | Raw data ingested as-is from ERP and CRM CSV files. No transformations — serves as the immutable source of truth. |
| 🥈 | **Silver** | Cleansed, standardised, and deduplicated data. Resolves quality issues and prepares data for integration. |
| 🥇 | **Gold** | Business-ready star schema. Fact and dimension views optimised for analytical queries and reporting. |

---

## 🗂️ Repository Structure

```
sql-data-warehouse-project/
│
├── 📂 datasets/                        # Raw source data files
│   ├── crm/                            # CRM system CSV files
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── erp/                            # ERP system CSV files
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
│
├── 📂 doc/                             # Architecture diagrams and documentation
│   ├── data_architecture.png           # Medallion architecture overview
│   ├── data_flow.png                   # End-to-end data flow diagram
│   ├── data_models.png                 # Star schema / ER diagram
│   ├── etl.png                         # ETL methods and techniques
│   ├── data_catalog.md                 # Field-level metadata for all tables
│   └── naming_conventions.md           # Naming standards for all objects
│
├── 📂 scripts/                         # All T-SQL scripts
│   ├── init_database.sql               # Database and schema initialisation
│   ├── bronze/                         # Raw ingestion layer
│   │   ├── ddl_bronze.sql              # Bronze table definitions
│   │   └── proc_load_bronze.sql        # Stored procedure: bulk load CSVs
│   ├── silver/                         # Cleansing and transformation layer
│   │   ├── ddl_silver.sql              # Silver table definitions
│   │   └── proc_load_silver.sql        # Stored procedure: transform and load
│   ├── gold/                           # Analytical model layer
│   │   └── ddl_gold.sql                # Gold views (star schema)
│   └── analysis/                       # Analysis scripts
│       ├── 01_exploratory_analysis.sql # Full structured EDA — 9 sections
│       └── 02_advanced_analysis.sql    # Advanced SQL analytics and report views
│
├── 📂 tests/                           # Data quality validation scripts
│   └── quality_checks.sql              # Row counts, nulls, referential checks
│
├── LICENSE
└── README.md
```

---

## 📐 Data Flow

The diagram below illustrates the complete journey of data from raw source files through each Medallion layer to final reporting.

![Data Flow](https://github.com/adetonayusuf/sql-data-warehouse-project/blob/main/doc/data-flow.gif)

---

## 🗃️ Data Model

The Gold layer is structured as a **Star Schema** - one central fact table joined to two dimension tables — optimised for fast analytical queries.

![Schema](https://github.com/adetonayusuf/sql-data-warehouse-project/blob/main/doc/Schema.png)


| Object | Type | Description |
|--------|------|-------------|
| `gold.dim_customers` | View | Customer profiles enriched with demographic data |
| `gold.dim_products` | View | Product catalogue with category, subcategory, and cost |
| `gold.fact_sales` | View | Sales transactions with keys, amounts, and quantities |

---

## ⚙️ ETL Pipeline

The ETL process loads and transforms data across all three layers via SQL Server stored procedures.

![Data Architecture](https://github.com/adetonayusuf/sql-data-warehouse-project/blob/main/doc/Data-Architecture.png)


### 🥉 Bronze — Raw Ingestion
Bulk loads all source CSV files into SQL Server with execution time logging and row count validation.
```sql
EXEC bronze.load_bronze;
```

### 🥈 Silver — Cleansing & Transformation
Applies data quality rules across all tables: deduplication, NULL handling, date standardisation, gender normalisation, and derived column enrichment.
```sql
EXEC silver.load_silver;
```

### 🥇 Gold — Analytical Model
Creates SQL views that expose a clean, business-ready star schema. No data is physically stored in Gold — views query Silver directly, ensuring the Gold layer is always current.
```sql
-- The Gold layer is built by running:
scripts/gold/ddl_gold.sql
```

---

## 🔍 Exploratory Data Analysis (EDA)

📄 **Script:** [`scripts/analysis/01_exploratory_analysis.sql`](scripts/analysis/01_exploratory_analysis.sql)

After the warehouse was built and loaded, a structured EDA was performed directly against the Gold layer. The analysis is divided into **9 sections**, each with a clear business purpose:

| Section | Analysis Area | What It Answers |
|---------|---------------|-----------------|
| 1 | **Database Exploration** | What tables and columns exist in the schema? |
| 2 | **Dimensions Exploration** | Which countries, categories, and products are in the data? |
| 3 | **Date Exploration** | What is the full trading period? How old are customers? |
| 4 | **Key Business Metrics** | What are the headline KPIs — revenue, orders, customers? |
| 5 | **Magnitude Analysis** | How do metrics break down by country, gender, category? |
| 6 | **Ranking Analysis** | Which products and customers are top and bottom performers? |
| 7 | **Time-Series & Trends** | How does revenue trend monthly, annually, cumulatively? |
| 8 | **Customer Segmentation** | Who are our High / Mid / Low value and Champion customers? |
| 9 | **Part-to-Whole Analysis** | What % of total revenue does each category contribute? |

### Key Techniques Used
- `DENSE_RANK()` for fair ranking with tie-handling
- `LAG()` for year-over-year comparisons
- `SUM() OVER()` for running totals and grand total calculations
- `AVG() OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)` for moving averages
- `CASE` statements for customer and product segmentation tiers
- `FORMAT()` and `DATETRUNC()` for clean time-series grouping

---

## 📊 Advanced SQL Analytics

📄 **Script:** [`scripts/analysis/02_advanced_analytics.sql`](scripts/analysis/02_advanced_analytics.sql)

Building on the EDA, this script delivers deeper analytical insights and two reusable reporting views that serve as the foundation for BI dashboards and stakeholder reports.

### Analyses Performed

**Sales Performance Over Time**
Monthly and yearly revenue trends using both `YEAR()` / `MONTH()` decomposition and `DATETRUNC()` for clean date grouping. Includes total revenue, unique customers, and quantity per period.

**Running Totals & Moving Average**
Cumulative revenue growth tracked month by month using `SUM() OVER()`. A true 3-year moving average of selling price computed using `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` to smooth price volatility over time.

**Yearly Product Performance**
Each product's annual sales benchmarked against two reference points simultaneously:
- Its own **historical average** across all years (using `AVG() OVER (PARTITION BY product_name)`)
- Its **prior year sales** (using `LAG() OVER (PARTITION BY product_name ORDER BY year)`)

Products are then classified as Above Avg / Below Avg and Increase / Decrease / No Change for fast trend identification.

**Part-to-Whole Analysis**
Revenue contribution percentage per category using `SUM() OVER()` to compute the grand total inline — revealing which categories dominate overall sales and where concentration risk exists.

**Data Segmentation**
- Products grouped into four cost bands: Below 100 / 100–500 / 500–1000 / Above 1000
- Customers classified into three tiers based on purchase lifespan and total spend:
  - **VIP** — 12+ months history and spend > £5,000
  - **Regular** — 12+ months history and spend ≤ £5,000
  - **New** — Less than 12 months of purchase history

---

### 📋 Report Views

Two reusable SQL views were built in the Gold layer to serve as permanent analytical assets:

#### `gold.report_customers`
A consolidated customer-level view that combines demographics, transaction history, segmentation, and KPIs into a single queryable object.

| Column | Description |
|--------|-------------|
| `customer_name` | Full name from first + last |
| `age` / `age_group` | Calculated age and banded age group |
| `customer_segment` | VIP / Regular / New based on lifespan and spend |
| `recency` | Months since the customer's last order |
| `total_orders` | Count of distinct orders placed |
| `total_sales` | Lifetime revenue generated |
| `total_quantity` | Total units purchased |
| `total_products` | Distinct products ever bought |
| `lifespan` | Months between first and last order |
| `avg_order_value` | Total sales ÷ total orders |
| `avg_monthly_spend` | Total sales ÷ lifespan in months |

#### `gold.report_products`
A consolidated product-level view combining catalogue attributes, sales performance, segmentation, and KPIs.

| Column | Description |
|--------|-------------|
| `product_name` / `category` / `subcategory` | Product hierarchy |
| `cost` | Unit cost from the product catalogue |
| `product_segment` | High-Performer / Mid-Range / Low-Performer |
| `recency_in_months` | Months since the product's last sale |
| `total_orders` | Count of distinct orders |
| `total_sales` | Total revenue generated |
| `total_quantity` | Total units sold |
| `total_customers` | Distinct customers who purchased the product |
| `lifespan` | Months between first and last sale |
| `avg_selling_price` | Weighted average price per unit sold |
| `avg_order_revenue` | Total sales ÷ total orders |
| `avg_monthly_revenue` | Total sales ÷ lifespan in months |

---

## 🚀 Getting Started

### Prerequisites

| Tool | Purpose | Link |
|------|---------|------|
| **SQL Server 2019+** | Core database engine | [Download](https://www.microsoft.com/en-us/sql-server/sql-server-downloads) |
| **SSMS** | SQL IDE and query execution | [Download](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms) |
| **Git** | Version control | [Download](https://git-scm.com/) |

### Step-by-Step Setup

**1. Clone the repository**
```bash
git clone https://github.com/adetonayusuf/sql-data-warehouse-project.git
cd sql-data-warehouse-project
```

**2. Initialise the database**
```sql
-- Creates the DataWarehouse database and bronze / silver / gold schemas
scripts/init_database.sql
```

**3. Load the Bronze Layer**
```sql
-- Bulk loads all ERP and CRM CSV files into bronze tables
EXEC bronze.load_bronze;
```

**4. Load the Silver Layer**
```sql
-- Cleanses, standardises, and loads data from bronze into silver
EXEC silver.load_silver;
```

**5. Build the Gold Layer**
```sql
-- Creates the star schema views in the gold schema
scripts/gold/ddl_gold.sql
```

**6. Run Data Quality Tests**
```sql
-- Validates row counts, null checks, duplicates, and referential integrity
tests/quality_checks.sql
```

**7. Run Exploratory Analysis**
```sql
-- 9-section structured EDA against the gold layer
scripts/analysis/01_exploratory_analysis.sql
```

**8. Run Advanced Analytics**
```sql
-- Advanced SQL analytics and reusable report views
scripts/analysis/02_advanced_analysis.sql
```

**9. Query the Report Views**
```sql
-- Preview the consolidated customer report
SELECT * FROM gold.report_customers;

-- Preview the consolidated product report
SELECT * FROM gold.report_products;
```

---

## 🛠️ Tools & Technologies

| Tool | Role |
|------|------|
| **SQL Server 2019+** | Core database and query engine |
| **T-SQL** | ETL stored procedures, window functions, CTEs, analytical queries |
| **SSMS** | Database management and script execution |
| **Draw.io** | Architecture, data flow, and data model diagrams |
| **Notion** | Full project planning — phases, tasks, and documentation management |
| **Git & GitHub** | Version control and portfolio hosting |

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| [`doc/data_catalog.md`](doc/data_catalog.md) | Field-level metadata, data types, and descriptions for all tables |
| [`doc/naming_conventions.md`](doc/naming_conventions.md) | Naming standards for schemas, tables, columns, and stored procedures |
| [`doc/data_Architecture.png`](doc/data_Architecture.png) | Medallion architecture overview diagram |
| [`doc/data_flow.gif`](doc/data_flow.gif) | End-to-end data flow from source to Gold |
| [`doc/schema.png`](doc/schema.png) | Star schema ER diagram |

---

## ✅ Project Specifications

| Requirement | Detail |
|-------------|--------|
| **Data Sources** | ERP and CRM systems provided as CSV flat files |
| **Data Quality** | Cleansed, deduplicated, and standardised in Silver layer |
| **Data Integration** | Unified into a star schema in the Gold layer |
| **Historisation** | Not required — latest snapshot only |
| **Analytical Model** | Star schema: 1 fact table + 2 dimension tables |
| **Exploratory Analysis** | 9-section structured EDA with window functions and segmentation |
| **Advanced Analytics** | Sales trends, YoY analysis, customer and product report views |
| **Reporting** | Gold views and report views ready for BI tools (Power BI, Tableau) |

---

## 📄 License

This project is licensed under the **MIT License** — free to use, modify, and distribute with proper attribution.

See the [LICENSE](LICENSE) file for full details.

---

## 👤 About the Author

**Adetona Yusuf Olalekan**
*Professional Accountant | Data Analyst & Data Engineer*

I'm passionate about turning raw data into actionable insights that drive organisational performance. With a background in accounting and a strong interest in data engineering, I build end-to-end data solutions from pipeline architecture through to analytical reporting that solve real business problems.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/yusuf-adetona/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/adetonayusuf)

---

⭐ *If you found this project useful or insightful, please consider giving it a star — it genuinely helps!*
