# Databricks notebook source
storage_account_name = "secondstorewella"
storage_account_access_key =dbutils.secrets.get(scope ="wella-secret-scope", key ="adlssecretKey")

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

# COMMAND ----------

dbutils.fs.ls("abfss://filecsv@secondstorewella.dfs.core.windows.net/csvdata/file")

# COMMAND ----------

paths = "abfss://filecsv@secondstorewella.dfs.core.windows.net/csvdata/file/*"

def get_df_from_csv_paths(paths):
  df_data = spark.read.format('csv') \
                 .option('header', 'true') \
                 .option('inferSchema', 'true') \
                 .option('delimiter', ',') \
                 .option('mode', 'DROPMALFORMED') \
                 .load(paths.split(','))
  return df_data


df_data.printSchema()


# COMMAND ----------

def get_df_from_csv_paths(paths):
  df_data = spark.read.format('csv') \
                 .option('header', 'true') \
                 .option('inferSchema', 'true') \
                 .option('delimiter', ',') \
                 .option('mode', 'DROPMALFORMED') \
                 .load(paths.split(','))
  return df_data


get_df_from_csv_paths.printSchema()
       

# COMMAND ----------

df_data.printSchema()

# COMMAND ----------

