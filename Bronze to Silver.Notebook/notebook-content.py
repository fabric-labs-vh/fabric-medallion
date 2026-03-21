# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d9cb1a7a-1b03-4c6c-ba0e-26bc822e46b5",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "851e1ae1-7097-4772-b330-584465b56abf",
# META       "known_lakehouses": [
# META         {
# META           "id": "d9cb1a7a-1b03-4c6c-ba0e-26bc822e46b5"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Extracting Data from the raw
# data in the folder raw was extracted from Azure SQL DB

# CELL ********************

#Get all the files under the ADLS folder and create a list of file paths
file_list = mssparkutils.fs.ls("abfss://medallion_architecture@onelake.dfs.fabric.microsoft.com/bronze.Lakehouse/Files/raw")
#Read each file and create a Dataframe
for file_path in file_list:
    print(file_path)
    df = spark.read.format("csv").options(inferSchema="true", header="true").load(path=f"{file_path.path}*")
    # You can process the Dataframe or register it as a table here
    # to create a temporary table:
    df.createOrReplaceTempView(file_path.name.removesuffix('.csv'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sql = spark.sql("SELECT * FROM salesltaddress LIMIT 100")
#display(df_sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

views = spark.sql("SHOW VIEWS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transforming & Normalizing Data

# MARKDOWN ********************


# CELL ********************

df_salesltsalesorderheader = spark.sql("SELECT SalesOrderID, RevisionNumber, OrderDate, DueDate, ShipDate, Status, OnlineOrderFlag, SalesOrderNumber, PurchaseOrderNumber, AccountNumber FROM salesltsalesorderheader")
#display(df_salesltsalesorderheader)
#Randomizing the dates in the OrderDate column since our toy AdventureWorks Li dataset only has one distinct order date.
from pyspark.sql.functions import rand, col, expr
df_salesltsalesorderheader = df_salesltsalesorderheader.drop("OrderDate").withColumn("OrderDate", expr("date_add(current_date()-1000, CAST(rand() * 365 AS INT))"))
#display(df_salesltsalesorderheader)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltsalesorderdetail =  spark.sql("SELECT SalesOrderID, OrderQty, ProductID, UnitPrice, UnitPriceDIscount, LineTotal FROM salesltsalesorderdetail")
#display(df_salesltsalesorderdetail)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesItcustomer = spark.sql("SELECT CustomerID, Title, FirstName, MiddleName, LastName, Suffix, CompanyName, EmailAddress, Phone FROM salesltcustomer")
#display(df_salesitcustomer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltcustomeraddress = spark.sql("SELECT CustomerID, AddressID, AddressType FROM salesltcustomeraddress") 
#display(df_salesltcustomeraddress)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltproduct = spark.sql("SELECT ProductID, Name, ProductNumber, Color, StandardCost, ListPrice, Size, Weight, ProductCategoryID FROM salesltproduct")
#display(df_salesltproduct)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_salesltproductcategory = spark.sql("SELECT ProductCategoryID, ParentProductCategoryID, Name FROM salesltproductcategory")
#displayidf salesitproductcategory)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Loading data for Silver layer tables

# CELL ********************

path = "abfss://medallion_architecture@onelake.dfs.fabric.microsoft.com/silver.Lakehouse/Tables" #to load for all 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "salesOrderHeader"
df_salesltsalesorderheader.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path +"/"+ tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesOrderDetail"
df_salesltsalesorderdetail.write.mode("overwrite").format("delta").option("overwriteSchema", "true").save(path + "/" + tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesCustomer"
df_salesItcustomer.write.mode("overwrite").format("delta").option("overwriteschema", "true").save(path+"/"+ tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "salesCustomerAddress"
df_salesltcustomeraddress.write.mode("overwrite").format("delta").option("overwriteschema", "true").save(path+"/"+ tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName = "salesProduct"
df_salesltproduct.write.mode("overwrite").format("delta").option("overwriteschema", "true").save(path+"/"+ tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tableName="salesProductCategory"
df_salesltproductcategory.write.mode("overwrite").format("delta").option("overwriteschema", "true").save(path+"/"+ tableName)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
