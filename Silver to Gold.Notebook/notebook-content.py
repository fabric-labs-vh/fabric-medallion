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
df_dimCustomer = spark.sql("select sc., sca.AddressID, sca. Address Type from salescustomer sc join salescustomeraddress sca on sc.customerid = sca.customerid")
#Add surrogate key as the first column.
df_dimCustomer_with_surrogate_key = df_dimCustomer.withColumn("Customer IDKey", monotonically_increasing_id())\
    .select(
        "Customer TDKey", #Select the surrogate key column first
        *[column for column in df dimCustomer.columns if column != "CustomerIDKey"] # Select the remaining columns in their origi
    )
df_dimCustomer_with_surrogate_key.createOrReplaceTempView('dimCustomer")\

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
