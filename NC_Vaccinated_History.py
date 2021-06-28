# Databricks notebook source
# MAGIC %md
# MAGIC # North Carolina Poeple Vaccinated By County
# MAGIC Notes: <br>
# MAGIC Federal: Vaccinated from Federal Pharmacy Programs <br>
# MAGIC NC: Vaccinated from NC Providers
# MAGIC <br>
# MAGIC Author: Zhongxiao Mei

# COMMAND ----------

vaccined_histroy = spark.read.csv('dbfs:/FileStore/tables/People_Vaccinated_by_County_Full_Data.csv', header=True, inferSchema=True)

# COMMAND ----------

vaccined_histroy.display()
vaccined_histroy.createOrReplaceTempView('vaccined_histroy_view')

# COMMAND ----------

# MAGIC %md
# MAGIC ### People Vaccinated in North Carolina (Fully & At Least one dose)

# COMMAND ----------

vaccined_fully = spark.sql("""
select County, `People Fully Vaccinated - Federal`+`People Fully Vaccinated - NC` as `People Fully Vaccinated By County`
from vaccined_histroy_view
order by `People Fully Vaccinated - NC` desc
""")
vaccined_fully.display()

# COMMAND ----------

vaccined_at_least_one = spark.sql("""
select County, `People Vaccinated with at Least One Dose - Federal`+`People Vaccinated with at Least One Dose - NC` as `People Vaccinated with at Least One Dose By County`
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

vaccinated_record = vaccined_fully.join(vaccined_at_least_one, vaccined_fully.County == vaccined_at_least_one.County, 'inner').select(vaccined_fully.County, 'People Fully Vaccinated By County', 'People Vaccinated with at Least One Dose By County')
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

People_Vaccinated_Record_With_NC_County_Map_Data = People_Vaccinated_Record_With_NC_County_long_lat.select('County','GEOID','`People Fully Vaccinated By County`','`People Vaccinated with at Least One Dose By County`')
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
    from_=alt.LookupData(data, 'GEOID', ['County', 'GEOID', 'People Fully Vaccinated By County','People Vaccinated with at Least One Dose By County'])
  ).encode(
    color=alt.Color('People Fully Vaccinated By County:Q', scale=alt.Scale(type='log', domain=[1000, 200000]), title='Vaccinated'),
    tooltip=[
      alt.Tooltip('GEOID:O'),
      alt.Tooltip('County:N'),
      alt.Tooltip('People Fully Vaccinated By County:Q'),
      alt.Tooltip('People Vaccinated with at Least One Dose By County:Q'),
    ],
  ).properties(
    #figure title
    title=f'People Vaccinated in North Carolina by County (Move your cursor to view detail)'
  )
  
  return (base_state + base_state_counties)

# COMMAND ----------

map_nc(nc_counties, People_Vaccinated_Record_With_NC_County_Map_Data)

# COMMAND ----------


