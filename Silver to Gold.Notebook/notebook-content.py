# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "de12c7d9-0184-46a1-9464-89a085c8fb4e",
# META       "default_lakehouse_name": "silver",
# META       "default_lakehouse_workspace_id": "851e1ae1-7097-4772-b330-584465b56abf",
# META       "known_lakehouses": [
# META         {
# META           "id": "de12c7d9-0184-46a1-9464-89a085c8fb4e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Get all the files under the ADLS folder and create a list of file paths
path_silver = "abfss://medallion_architecture@onelake.dfs.fabric.microsoft.com/silver.Lakehouse/Tables"

file_list = mssparkutils.fs.ls(path_silver)

for file_path in file_list:
    if file_path.name.startswith("_") or file_path.name == "dbo":
        continue

    table_name = file_path.name.rstrip("/")
    print(f"Lendo tabela: {table_name}")

    df = spark.read.format("delta").load(file_path.path)
    df.createOrReplaceTempView(table_name)

    print(f"  ✅ TempView '{table_name}' criada")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id

#Create DimCustomer
df_dimCustomer = spark.sql("select sc.* , sca.AddressID, sca.AddressType from salescustomer sc join salescustomeraddress sca on sc.customerid = sca.customerid")
# Add surrogate key as the first column.
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn("CustomerIDKey", monotonically_increasing_id())\
    .select(
        "CustomerIDKey", # Select the surrogate key column first
        *[column for column in df_dimCustomer.columns if column != "CustomerIDKey"] # Select the remaining columns in their origin
    )
df_dimCustomer_with_surrogate_key.createOrReplaceTempView('dimCustomer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Create dim Product
df_dimProduct = spark.sql("select sp.*, spc.ParentProductCategoryID, spc.Name as ProductCategoryName from salesproduct sp join salesproductcategory spc on sp.ProductCategoryID = spc.ProductCategoryID")
# Add surrogate key as the first column
df_dimProduct_with_surrogate_key = df_dimProduct.withColumn("ProductIDKey", monotonically_increasing_id())\
    .select(
        "ProductIDKey", # Select the surrogate key column first
        *[column for column in df_dimProduct.columns if column != "ProductIDKey" and column != "spc.Name"] # Select the remaining columns in their original order
    )  
df_dimProduct_with_surrogate_key.createOrReplaceTempView('dimProduct')
    #display(df_dimProduct with surrogate key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import expr
# Define the start and end dates for your Dimbate table
start_date = "2000-01-01"
end_date = "2024-12-31"

#Create a DataFrame with a range of dates
df_dimDate = spark.range(0, (spark.sql("SELECT datediff('{0}', '{1}')".format(end_date, start_date)).collect()[0][0])+1) \
    .selectExpr("CAST(id AS INT) AS id") \
    .selectExpr("date_add('{0}', id) As Date".format(start_date))
#Extract different date components
df_dimDate = df_dimDate \
    .withColumn("Year", expr("Year(Date)")) \
    .withColumn("Month", expr("Month(Date)")) \
    .withColumn("DayOfMonth", expr("DayOfMonth(Date)")) \
    .withColumn("DayOfYear", expr("DayOfYear(Date)")) \
    .withColumn("WeekOfYear", expr("WeekOfYear(Date)")) \
    .withColumn("DayOfWeek", expr("DayOfWeek(Date)")) \
    .withColumn("Quarter", expr("Quarter(Date)"))
#display(df_dimDate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.sql("SELECT * FROM salesOrderHeader limit 2")
df2 = spark.sql("SELECT * FROM salesOrderDetail limit 2")

#display(df1)
#display(df2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## to gold

# CELL ********************


#spark.table("salesCustomer").printSchema()
#spark.table("salesCustomerAddress").printSchema()
#spark.table("salesOrderDetail").printSchema()
#spark.table("salesOrderHeader").printSchema()
#spark.table("salesProduct").printSchema()
#spark.table("salesProductCategory").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_factSales = spark.sql("SELECT soh.*, sca.AddressID as BillToAddressID, sc.CustomerID, ds.CustomerIDKey, sod.OrderQty, sod.ProductID, sod.UnitPrice, sod.UnitPriceDIscount, sod.LineTotal, dp.ProductIDKey FROM salesorderheader soh JOIN salesorderdetail sod ON soh.SalesOrderID = sod.SalesOrderID JOIN dimProduct dp ON sod.ProductID = dp.ProductID JOIN salescustomeraddress sca ON soh.AccountNumber = sca.AddressID JOIN salescustomer sc ON sca.CustomerID = sc.CustomerID JOIN dimCustomer ds ON sc.CustomerID = ds.CustomerID")
#display(df_factSales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

path = "abfss://medallion_architecture@onelake.dfs.fabric.microsoft.com/gold.Lakehouse/Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "fabric_dimProduct"
df_dimProduct_with_surrogate_key.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path + "/" + tableName)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dbo.{tableName} USING DELTA LOCATION '{path}/{tableName}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "fabric_dimCustomer"
df_dimCustomer_with_surrogate_key.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path + "/" + tableName)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dbo.{tableName} USING DELTA LOCATION '{path}/{tableName}'")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "fabric_dimDate"
df_dimDate.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path + "/" + tableName)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dbo.{tableName} USING DELTA LOCATION '{path}/{tableName}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "fabric_factSales"
df_factSales.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path + "/" + tableName)
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dbo.{tableName} USING DELTA LOCATION '{path}/{tableName}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
