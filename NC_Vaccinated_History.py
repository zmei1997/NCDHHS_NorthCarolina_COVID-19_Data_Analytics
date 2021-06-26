# Databricks notebook source
# MAGIC %md
# MAGIC # North Carolina Poeple Vaccinated By County
# MAGIC Notes: <br>
# MAGIC Federal: Vaccinated from Federal Pharmacy Programs <br>
# MAGIC NC: Vaccinated from NC Providers

# COMMAND ----------

vaccined_histroy = spark.read.csv('dbfs:/FileStore/tables/People_Vaccinated_by_County_Full_Data.csv', header=True, inferSchema=True)

# COMMAND ----------

vaccined_histroy.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### People Fully Vaccinated in North Carolina

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   County,
# MAGIC   `People Fully Vaccinated - NC`
# MAGIC from
# MAGIC   vaccined_histroy_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   sum(`People Fully Vaccinated - Federal`) as `Total Number Of People Fully Vaccinated - Federal`,
# MAGIC   sum(`People Fully Vaccinated - NC`) as `Total Number Of People Fully Vaccinated - NC`
# MAGIC from
# MAGIC   vaccined_histroy_table

# COMMAND ----------

# MAGIC %md
# MAGIC #### People Fully Vaccinated - NC Providers (Top10 Counties)

# COMMAND ----------

vaccined_histroy.createOrReplaceTempView('vaccined_histroy_table')
vaccined_NC_providers = spark.sql("""
select County, `People Fully Vaccinated - NC` as `People Fully Vaccinated By County NC Providers`
from vaccined_histroy_table
order by `People Fully Vaccinated - NC` desc
limit 10
""")
vaccined_NC_providers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### People Fully Vaccinated - Federal Pharmacy Programs (Top10 Counties)

# COMMAND ----------

vaccined_Federal_Pharmacy_Programs = spark.sql("""
select County, `People Fully Vaccinated - Federal` as `People Fully Vaccinated By County Federal Pharmacy Programs`
from vaccined_histroy_table
order by `People Fully Vaccinated - Federal` desc
limit 10
""")
vaccined_Federal_Pharmacy_Programs.display()

# COMMAND ----------


