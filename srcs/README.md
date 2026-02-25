# PySpark Transformation Script

## Overview
This script implements all the data transformation requirements for the Insight2Profit Data Engineering Assessment.

## Requirements
- Python 3.7+
- PySpark 3.3.0+

## Installation
```bash
pip install -r requirements.txt
```

## Usage
Run the script from the parent directory (where the CSV files are located):
```bash
cd ..
python srcs/main.py
```

Or using spark-submit:
```bash
spark-submit srcs/main.py
```

## What the Script Does

### Task 1: Data Loading
- Loads three CSV files (products, sales_order_header, sales_order_detail)
- Creates temporary views with `raw_` prefix

### Task 2: Data Review and Storage
- Assigns appropriate data types to all columns
- Identifies primary and foreign keys
- Stores transformed data with `store_` prefix

### Task 3: Product Master Transformations
- Replaces NULL values in Color field with 'N/A'
- Enhances ProductCategoryName based on ProductSubCategoryName logic
- Creates `publish_product` table

### Task 4: Sales Order Transformations
- Joins SalesOrderDetail with SalesOrderHeader
- Calculates LeadTimeInBusinessDays (excluding weekends)
- Calculates TotalLineExtendedPrice
- Creates `publish_orders` table

### Task 5: Analysis Questions
Answers two key business questions:
1. Which color generated the highest revenue each year?
2. What is the average LeadTimeInBusinessDays by ProductCategoryName?

## Output
The script will display:
- Progress messages for each task
- Analysis results with formatted tables
- Sample data previews from the final tables
