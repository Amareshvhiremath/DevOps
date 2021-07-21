# Databricks notebook source
storage_account_name = "secondstorewella"
#storage_account_access_key = "/YJtqcXVctnA8EMACl2ZeKDZ2MNvZL7p9u6hpVesN4ulKuQlZJBVQ0zVssHwdeLiQBiiIpetLnSbUqs8PnJdYg=="

storage_account_access_key =dbutils.secrets.get(scope ="wella-secret-scope", key ="adlssecretKey")

# COMMAND ----------

# establishing an execution context
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

storage_type = "abfss:/"
file_container = "/filecsv"

file_folderlocation = "/csvdata/file/"

file_type = "csv"
file_wildcard = "new*"

#file_loadpath = storage_type + file_container + "@" + storage_account_name + ".dfs.core.windows.net" + file_folderlocation + file_wildcard

file_loadpath = 'abfss://filecsv@secondstorewella.dfs.core.windows.net/csvdata/file/new*'

# COMMAND ----------

df = spark.read.format(file_type) \
  .option("inferSchema", "True") \
  .option("header", "True") \
  .load(file_loadpath)

display(df)

# COMMAND ----------

from pyspark.sql.types import (StructField, IntegerType, StringType, DateType, StructType)

data_schema = [StructField('Names', StringType(), True),
               StructField('Age', IntegerType(),True),
               StructField('Total_Purchase', IntegerType(), True),
               StructField('Account_Manager', IntegerType(), True),
               StructField('Years', IntegerType(), True),
               StructField('Num_Sites', IntegerType(), True),
               StructField('Onboard_Date', DateType(), True),
               StructField('Location', StringType(), False),
               StructField('Company', StringType(), False)]



# COMMAND ----------

final_struct = StructType(fields=data_schema)

# COMMAND ----------

schema_df = spark.read.csv("new_customers.csv", schema=final_struct)

# COMMAND ----------

# DBTITLE 1,Reading and writing of data in adls gen2 

