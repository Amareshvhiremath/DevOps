# Databricks notebook source
dbutils.fs.unmount("/mnt/data")

# COMMAND ----------

configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
          "fs.adl.oauth2.client.id": "99674ddc-21fa-4454-9b50-8b4d9fd20c1c",
          "fs.adl.oauth2.credential": "f87970bb-a0a1-4468-b0ca-ae77422edfb5",
          "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/3882b70d-a91e-468c-9928-820358bfbd73/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://datalake@wellapractice.dfs.core.window.net/",
  mount_point = "/mnt/data",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls "mnt/data"

# COMMAND ----------

spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("fs.adl.oauth2.client.id", "99674ddc-21fa-4454-9b50-8b4d9fd20c1c")
spark.conf.set("fs.adl.oauth2.credential", "f87970bb-a0a1-4468-b0ca-ae77422edfb5")
spark.conf.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/3882b70d-a91e-468c-9928-820358bfbd73/oauth2/token")

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "adl://wellapractice.azuredatalakestore.net/data/customer",
  mount_point = "/mnt/data",
  extra_configs = configs)



# COMMAND ----------

# MAGIC %fs ls "/mnt/data"

# COMMAND ----------

df = spark.read.text("/mnt/data/customer/new_customers.csv")
df.show();

# COMMAND ----------

##path="/mnt/data/customer/new_customers.csv"

##df=spark.read.format("csv").option("header","false").option("inferSchema","false").load(path)

# df = spark.read.text("/mnt/data/c")



# COMMAND ----------

# MAGIC %fs ls "abfss://datalake@wellapractice.dfs.core.window.net"

# COMMAND ----------

