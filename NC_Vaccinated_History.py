# Databricks notebook source
# MAGIC %md
# MAGIC # North Carolina People Vaccinated By County
# MAGIC Notes: <br>
# MAGIC Federal: Vaccinated from Federal Pharmacy Programs <br>
# MAGIC NC: Vaccinated from NC Providers
# MAGIC <br>
# MAGIC Author: Zhongxiao Mei

# COMMAND ----------

vaccined_histroy = spark.read.csv('dbfs:/FileStore/tables/People_Vaccinated_by_County_Full_Data.csv', header=True, inferSchema=True)

# COMMAND ----------

vaccined_histroy.printSchema()

# COMMAND ----------

Total_Doses_Administered_By_County = spark.read.csv('dbfs:/FileStore/tables/Doses_By_County_Full_Data.csv', header=True, inferSchema=True)

# COMMAND ----------

Total_Doses_Administered_By_County.printSchema()

# COMMAND ----------

Total_Doses_Administered_By_County.display()
Total_Doses_Administered_By_County.createOrReplaceTempView('total_Doses_Administered_view')

# COMMAND ----------

vaccined_histroy.display()
vaccined_histroy.createOrReplaceTempView('vaccined_histroy_view')

# COMMAND ----------

# MAGIC %md
# MAGIC ### People Vaccinated in North Carolina (Fully & At Least one dose)

# COMMAND ----------

vaccined_fully = spark.sql("""
select vv.County, `People Fully Vaccinated - Federal`+`People Fully Vaccinated - NC` as `People Fully Vaccinated`, `Total Doses Administered`
from vaccined_histroy_view vv inner join total_Doses_Administered_view tv on vv.County = tv.County
order by `People Fully Vaccinated - NC` desc
""")
vaccined_fully.display()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import ceil
vaccined_fully = vaccined_fully.withColumn('Percentage of Fully Vaccinated', col('`People Fully Vaccinated`')/ col('`Total Doses Administered`')*100)
vaccined_fully = vaccined_fully.select('County', '`People Fully Vaccinated`', '`Total Doses Administered`', ceil(col('`Percentage of Fully Vaccinated`')).alias('Percentage of Fully Vaccinated (%)'))
vaccined_fully.display()

# COMMAND ----------

vaccined_at_least_one = spark.sql("""
select County, `People Vaccinated with at Least One Dose - Federal`+`People Vaccinated with at Least One Dose - NC` as `People Vaccinated with at Least One Dose`
from vaccined_histroy_view
order by `People Fully Vaccinated - Federal` desc
""")
vaccined_at_least_one.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   sum(`People Fully Vaccinated - Federal`) + sum(`People Fully Vaccinated - NC`) as `Total Number Of People Fully Vaccinated`,
# MAGIC   sum(`People Vaccinated with at Least One Dose - Federal`) + sum(`People Vaccinated with at Least One Dose - NC`) as `Total Number Of People Vaccinated with at Least One Dose`
# MAGIC from
# MAGIC   vaccined_histroy_view

# COMMAND ----------

# MAGIC %md
# MAGIC #### People Fully Vaccinated By County (Top10 Counties)

# COMMAND ----------

vaccined_fully_top10 = vaccined_fully.limit(10)
vaccined_fully_top10.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### People Vaccinated with at Least One Dose By County (Top10 Counties)

# COMMAND ----------

vaccined_at_least_one_top10 = vaccined_at_least_one.limit(10)
vaccined_at_least_one_top10.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read NC Counties' Geo_id, Longitute and latitute data from text file

# COMMAND ----------

schema='USPS string,GEOID integer,ANSICODE integer,NAME string,ALAND long,AWATER long,ALAND_SQMI double,AWATER_SQMI double,INTPTLAT double,INTPTLONG double'
nc_counties_long_lat = spark.read.schema(schema).csv('dbfs:/FileStore/tables/2020_gaz_counties_37.txt', header=True, sep="\t")
nc_counties_long_lat.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transform data

# COMMAND ----------

vaccinated_record = vaccined_fully.join(vaccined_at_least_one, vaccined_fully.County == vaccined_at_least_one.County, 'inner').select(vaccined_fully.County, 'People Vaccinated with at Least One Dose', 'People Fully Vaccinated', 'Total Doses Administered', 'Percentage of Fully Vaccinated (%)')
vaccinated_record.display()
vaccinated_record.createOrReplaceTempView('vaccinated_record_view')

# COMMAND ----------

from pyspark.sql.functions import split
from pyspark.sql.functions import trim

nc_counties_long_lat = nc_counties_long_lat.select('USPS','GEOID',trim(split('Name', ' ', -1)[0]).alias('Name'),'INTPTLAT','INTPTLONG')
nc_counties_long_lat.display()
nc_counties_long_lat.createOrReplaceTempView('nc_counties_long_lat_view')

# COMMAND ----------

# MAGIC %md
# MAGIC #### People Vaccinated Record With NC County GEOID, longitute and latitute
# MAGIC Combine People Vaccinated table and NC County longitute latitute table

# COMMAND ----------

People_Vaccinated_Record_With_NC_County_long_lat = spark.sql("""
select *
from vaccinated_record_view v inner join nc_counties_long_lat_view c on v.County = c.Name
""")
People_Vaccinated_Record_With_NC_County_long_lat = People_Vaccinated_Record_With_NC_County_long_lat.drop('USPS', 'Name')
People_Vaccinated_Record_With_NC_County_long_lat.display()

# COMMAND ----------

# MAGIC %pip install altair

# COMMAND ----------

import altair as alt

# COMMAND ----------

People_Vaccinated_Record_With_NC_County_Map_Data = People_Vaccinated_Record_With_NC_County_long_lat.select('County','GEOID','`People Vaccinated with at Least One Dose`', '`People Fully Vaccinated`', 'Total Doses Administered', 'Percentage of Fully Vaccinated (%)')
People_Vaccinated_Record_With_NC_County_Map_Data.display()
People_Vaccinated_Record_With_NC_County_Map_Data = People_Vaccinated_Record_With_NC_County_Map_Data.toPandas()

# COMMAND ----------

topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_nc = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/NC-37-north-carolina-counties.json'
us_counties = alt.topo_feature(topo_usa, 'counties')
nc_counties = alt.topo_feature(topo_nc, 'counties_nc')

# COMMAND ----------

def map_nc(counties, data):
  # State
  base_state = alt.Chart(counties).mark_geoshape(
      fill='white',
      stroke='lightgray',
  ).properties(
      width=800,
      height=600,
  ).project(
      type='mercator'
  )
  
  # counties
  base_state_counties = alt.Chart(us_counties).mark_geoshape(
  ).transform_lookup(
    lookup='id',
    from_=alt.LookupData(data, 'GEOID', ['County', 'GEOID', 'People Vaccinated with at Least One Dose', 'People Fully Vaccinated', 'Total Doses Administered', 'Percentage of Fully Vaccinated (%)'])
  ).encode(
    color=alt.Color('People Fully Vaccinated:Q', scale=alt.Scale(type='log', domain=[1000, 1000000]), title='Vaccinated'),
    tooltip=[
      alt.Tooltip('GEOID:O'),
      alt.Tooltip('County:N'),
      alt.Tooltip('People Vaccinated with at Least One Dose:Q'),
      alt.Tooltip('People Fully Vaccinated:Q'),
      alt.Tooltip('Total Doses Administered:Q'),
      alt.Tooltip('Percentage of Fully Vaccinated (%):Q'),
    ],
  ).properties(
    #figure title
    title=f'People Vaccinated in North Carolina by County (Move your cursor to view detail)'
  )
  
  return (base_state + base_state_counties)

# COMMAND ----------

map_nc(nc_counties, People_Vaccinated_Record_With_NC_County_Map_Data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Vaccinated in North Carolina
# MAGIC #### Data updated on July, 2, 2021

# COMMAND ----------

us_daily_vaccinated = spark.read.csv('dbfs:/FileStore/tables/us_state_vaccinations.csv', header=True, inferSchema=True)
us_daily_vaccinated.display()

# COMMAND ----------

nc_daily_vaccinated = us_daily_vaccinated.where(us_daily_vaccinated.location == 'North Carolina').where(us_daily_vaccinated.date >= )
nc_daily_vaccinated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Vaccinated in North Carolina (Year 2021)

# COMMAND ----------

nc_daily_vaccinated.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Vaccinated in North Carolina (Since May, 2021)

# COMMAND ----------

nc_daily_vaccinated_since_may = nc_daily_vaccinated.where(nc_daily_vaccinated.date > '2021-05-01')
nc_daily_vaccinated_since_may.display()

# COMMAND ----------


