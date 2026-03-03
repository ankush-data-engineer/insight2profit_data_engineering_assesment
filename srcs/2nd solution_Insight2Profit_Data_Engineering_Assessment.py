"""
Insight2Profit Data Engineering Assessment
PySpark transformation script for sales and product data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, coalesce, datediff, 
    sum as _sum, avg, year, expr, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, BooleanType, DateType
)


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("Insight2Profit Data Engineering Assessment") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()


def load_raw_data(spark):
    """
    Task 1: Load the provided three files with raw_ prefix
    """
    # Define schema for products
    products_schema = StructType([
        StructField("ProductID", IntegerType(), False),
        StructField("ProductDesc", StringType(), True),
        StructField("ProductNumber", StringType(), True),
        StructField("MakeFlag", StringType(), True),
        StructField("Color", StringType(), True),
        StructField("SafetyStockLevel", IntegerType(), True),
        StructField("ReorderPoint", IntegerType(), True),
        StructField("StandardCost", DecimalType(19, 4), True),
        StructField("ListPrice", DecimalType(19, 4), True),
        StructField("Size", StringType(), True),
        StructField("SizeUnitMeasureCode", StringType(), True),
        StructField("Weight", DecimalType(8, 2), True),
        StructField("WeightUnitMeasureCode", StringType(), True),
        StructField("ProductCategoryName", StringType(), True),
        StructField("ProductSubCategoryName", StringType(), True)
    ])
    
    # Define schema for sales_order_header
    header_schema = StructType([
        StructField("SalesOrderID", IntegerType(), False),
        StructField("OrderDate", StringType(), True),
        StructField("ShipDate", StringType(), True),
        StructField("OnlineOrderFlag", StringType(), True),
        StructField("AccountNumber", StringType(), True),
        StructField("CustomerID", IntegerType(), True),
        StructField("SalesPersonID", StringType(), True),
        StructField("Freight", DecimalType(19, 4), True)
    ])
    
    # Define schema for sales_order_detail
    detail_schema = StructType([
        StructField("SalesOrderID", IntegerType(), False),
        StructField("SalesOrderDetailID", IntegerType(), False),
        StructField("OrderQty", IntegerType(), True),
        StructField("ProductID", IntegerType(), True),
        StructField("UnitPrice", DecimalType(19, 4), True),
        StructField("UnitPriceDiscount", DecimalType(19, 4), True)
    ])
    
    # Load raw data
    raw_products = spark.read.csv("/Workspace/Users/educateankush@gmail.com/insight2profit_data_engineering_assesment/products.csv", header=True, schema=products_schema)
    raw_sales_header = spark.read.csv("/Workspace/Users/educateankush@gmail.com/insight2profit_data_engineering_assesment/sales_order_header.csv", header=True, schema=header_schema)
    raw_sales_detail = spark.read.csv("/Workspace/Users/educateankush@gmail.com/insight2profit_data_engineering_assesment/sales_order_detail.csv", header=True, schema=detail_schema)
    
    # Create temp views with raw_ prefix
    raw_products.createOrReplaceTempView("raw_products")
    raw_sales_header.createOrReplaceTempView("raw_sales_header")
    raw_sales_detail.createOrReplaceTempView("raw_sales_detail")
    
    print("✓ Raw data loaded successfully")
    return raw_products, raw_sales_header, raw_sales_detail


def store_transformed_data(spark, raw_products, raw_sales_header, raw_sales_detail):
    """
    Task 2: Review data, assign appropriate data types, and store with store_ prefix
    Primary Keys: ProductID, SalesOrderID, SalesOrderDetailID
    Foreign Keys: ProductID in sales_detail, SalesOrderID in sales_detail
    """
    # Transform products with proper data types
    store_products = raw_products.select(
        col("ProductID").cast(IntegerType()),
        trim(col("ProductDesc")).alias("ProductDesc"),
        trim(col("ProductNumber")).alias("ProductNumber"),
        when(col("MakeFlag") == "True", True).otherwise(False).alias("MakeFlag"),
        trim(col("Color")).alias("Color"),
        col("SafetyStockLevel").cast(IntegerType()),
        col("ReorderPoint").cast(IntegerType()),
        col("StandardCost").cast(DecimalType(19, 4)),
        col("ListPrice").cast(DecimalType(19, 4)),
        trim(col("Size")).alias("Size"),
        trim(col("SizeUnitMeasureCode")).alias("SizeUnitMeasureCode"),
        col("Weight").cast(DecimalType(8, 2)),
        trim(col("WeightUnitMeasureCode")).alias("WeightUnitMeasureCode"),
        trim(col("ProductCategoryName")).alias("ProductCategoryName"),
        trim(col("ProductSubCategoryName")).alias("ProductSubCategoryName")
    )
    
    # Transform sales_order_header with proper data types
    store_sales_header = raw_sales_header.select(
        col("SalesOrderID").cast(IntegerType()),
        col("OrderDate").cast(DateType()),
        col("ShipDate").cast(DateType()),
        when(col("OnlineOrderFlag") == "True", True).otherwise(False).alias("OnlineOrderFlag"),
        trim(col("AccountNumber")).alias("AccountNumber"),
        col("CustomerID").cast(IntegerType()),
        col("SalesPersonID").cast(IntegerType()),
        col("Freight").cast(DecimalType(19, 4))
    )
    
    # Transform sales_order_detail with proper data types
    store_sales_detail = raw_sales_detail.select(
        col("SalesOrderID").cast(IntegerType()),
        col("SalesOrderDetailID").cast(IntegerType()),
        col("OrderQty").cast(IntegerType()),
        col("ProductID").cast(IntegerType()),
        col("UnitPrice").cast(DecimalType(19, 4)),
        col("UnitPriceDiscount").cast(DecimalType(19, 4))
    )
    
    # Create temp views with store_ prefix
    store_products.createOrReplaceTempView("store_products")
    store_sales_header.createOrReplaceTempView("store_sales_header")
    store_sales_detail.createOrReplaceTempView("store_sales_detail")
    
    print("✓ Data stored with proper types and store_ prefix")
    return store_products, store_sales_header, store_sales_detail


def transform_product_master(spark, store_products):
    """
    Task 3: Product Master Transformations
    """
    publish_product = store_products.select(
        col("ProductID"),
        col("ProductDesc"),
        col("ProductNumber"),
        col("MakeFlag"),
        # Replace NULL values in Color with 'N/A'
        coalesce(col("Color"), lit("N/A")).alias("Color"),
        col("SafetyStockLevel"),
        col("ReorderPoint"),
        col("StandardCost"),
        col("ListPrice"),
        col("Size"),
        col("SizeUnitMeasureCode"),
        col("Weight"),
        col("WeightUnitMeasureCode"),
        # Enhance ProductCategoryName when NULL
        when(
            col("ProductCategoryName").isNull() | (trim(col("ProductCategoryName")) == ""),
            when(
                col("ProductSubCategoryName").isin(['Gloves', 'Shorts', 'Socks', 'Tights', 'Vests']),
                lit("Clothing")
            ).when(
                col("ProductSubCategoryName").isin(['Locks', 'Lights', 'Headsets', 'Helmets', 'Pedals', 'Pumps']),
                lit("Accessories")
            ).when(
                col("ProductSubCategoryName").contains("Frames") | 
                col("ProductSubCategoryName").isin(['Wheels', 'Saddles']),
                lit("Components")
            ).otherwise(col("ProductCategoryName"))
        ).otherwise(col("ProductCategoryName")).alias("ProductCategoryName"),
        col("ProductSubCategoryName")
    )
    
    publish_product.createOrReplaceTempView("publish_product")
    print("✓ Product master transformations completed")
    return publish_product


def calculate_business_days(order_date, ship_date):
    """
    Calculate business days between two dates (excluding weekends)
    Using SQL expression for business days calculation
    """
    return expr(f"""
        CASE 
            WHEN {ship_date} IS NULL OR {order_date} IS NULL THEN NULL
            ELSE datediff({ship_date}, {order_date}) - 
                 (CASE WHEN dayofweek({order_date}) = 1 THEN 1 ELSE 0 END) -
                 (CASE WHEN dayofweek({ship_date}) = 7 THEN 1 ELSE 0 END) -
                 (floor(datediff({ship_date}, {order_date}) / 7) * 2)
        END
    """)


def transform_sales_orders(spark, store_sales_detail, store_sales_header):
    """
    Task 4: Sales Order Transformations
    """
    # Join sales detail with header
    joined_df = store_sales_detail.join(
        store_sales_header,
        on="SalesOrderID",
        how="inner"
    )
    
    # Apply transformations
    publish_orders = joined_df.select(
        # All fields from SalesOrderDetail
        col("SalesOrderDetailID"),
        col("OrderQty"),
        col("ProductID"),
        col("UnitPrice"),
        col("UnitPriceDiscount"),
        # All fields from SalesOrderHeader except SalesOrderId
        col("OrderDate"),
        col("ShipDate"),
        col("OnlineOrderFlag"),
        col("AccountNumber"),
        col("CustomerID"),
        col("SalesPersonID"),
        # Rename Freight to TotalOrderFreight
        col("Freight").alias("TotalOrderFreight"),
        # Calculate LeadTimeInBusinessDays
        expr("""
            datediff(ShipDate, OrderDate) - 
            floor(datediff(ShipDate, OrderDate) / 7) * 2 -
            CASE WHEN dayofweek(OrderDate) = 1 THEN 1 ELSE 0 END -
            CASE WHEN dayofweek(ShipDate) = 7 THEN 1 ELSE 0 END
        """).alias("LeadTimeInBusinessDays"),
        # Calculate TotalLineExtendedPrice
        (col("OrderQty") * (col("UnitPrice") - col("UnitPriceDiscount"))).alias("TotalLineExtendedPrice")
    )
    
    publish_orders.createOrReplaceTempView("publish_orders")
    print("✓ Sales order transformations completed")
    return publish_orders


def analysis_questions(spark):
    """
    Task 5: Analysis Questions
    """
    print("\n" + "="*80)
    print("ANALYSIS RESULTS")
    print("="*80)
    
    # Question 1: Which color generated the highest revenue each year?
    print("\n1. Highest Revenue Color by Year:")
    print("-" * 80)
    
    revenue_by_color_year = spark.sql("""
        SELECT 
            year(po.OrderDate) as Year,
            pp.Color,
            SUM(po.TotalLineExtendedPrice) as TotalRevenue
        FROM publish_orders po
        JOIN publish_product pp ON po.ProductID = pp.ProductID
        WHERE pp.Color IS NOT NULL AND pp.Color != 'N/A'
        GROUP BY year(po.OrderDate), pp.Color
    """)
    
    highest_revenue_color = spark.sql("""
        WITH revenue_by_color_year AS (
            SELECT 
                year(po.OrderDate) as Year,
                pp.Color,
                SUM(po.TotalLineExtendedPrice) as TotalRevenue
            FROM publish_orders po
            JOIN publish_product pp ON po.ProductID = pp.ProductID
            WHERE pp.Color IS NOT NULL AND pp.Color != 'N/A'
            GROUP BY year(po.OrderDate), pp.Color
        ),
        ranked_colors AS (
            SELECT 
                Year,
                Color,
                TotalRevenue,
                ROW_NUMBER() OVER (PARTITION BY Year ORDER BY TotalRevenue DESC) as rank
            FROM revenue_by_color_year
        )
        SELECT Year, Color, ROUND(TotalRevenue, 2) as TotalRevenue
        FROM ranked_colors
        WHERE rank = 1
        ORDER BY Year
    """)
    
    highest_revenue_color.show(truncate=False)
    
    # Question 2: Average LeadTimeInBusinessDays by ProductCategoryName
    print("\n2. Average Lead Time (Business Days) by Product Category:")
    print("-" * 80)
    
    avg_lead_time = spark.sql("""
        SELECT 
            pp.ProductCategoryName,
            ROUND(AVG(po.LeadTimeInBusinessDays), 2) as AvgLeadTimeInBusinessDays,
            COUNT(*) as OrderCount
        FROM publish_orders po
        JOIN publish_product pp ON po.ProductID = pp.ProductID
        WHERE pp.ProductCategoryName IS NOT NULL 
          AND po.LeadTimeInBusinessDays IS NOT NULL
        GROUP BY pp.ProductCategoryName
        ORDER BY AvgLeadTimeInBusinessDays DESC
    """)
    
    avg_lead_time.show(truncate=False)
    print("="*80 + "\n")


def main():
    """Main execution function"""
    print("\n" + "="*80)
    print("INSIGHT2PROFIT DATA ENGINEERING ASSESSMENT")
    print("="*80 + "\n")
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Task 1: Load raw data
        print("Task 1: Loading raw data...")
        raw_products, raw_sales_header, raw_sales_detail = load_raw_data(spark)
        
        # Task 2: Store transformed data with proper types
        print("\nTask 2: Storing data with proper types...")
        store_products, store_sales_header, store_sales_detail = store_transformed_data(
            spark, raw_products, raw_sales_header, raw_sales_detail
        )
        
        # Task 3: Product master transformations
        print("\nTask 3: Applying product master transformations...")
        publish_product = transform_product_master(spark, store_products)
        
        # Task 4: Sales order transformations
        print("\nTask 4: Applying sales order transformations...")
        publish_orders = transform_sales_orders(spark, store_sales_detail, store_sales_header)
        
        # Task 5: Analysis questions
        print("\nTask 5: Running analysis...")
        analysis_questions(spark)
        
        print("✓ All transformations completed successfully!\n")
        
        # Optional: Show sample data
        print("Sample Data Preview:")
        print("-" * 80)
        print("\nPublish Product (first 5 rows):")
        publish_product.show(5, truncate=False)
        
        print("\nPublish Orders (first 5 rows):")
        publish_orders.show(5, truncate=False)
        
    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        raise
    # Removed spark.stop() from finally block


if __name__ == "__main__":
    main()
