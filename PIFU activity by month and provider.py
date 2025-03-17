# Databricks notebook source
from env import env
from src import utils

from pyspark.sql import functions as F

# COMMAND ----------

#Load PIFU data
df_raw_pifu = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(env["pifu_path"])
#Temp view holding PIFU table data
df_raw_pifu.createOrReplaceGlobalTempView("RawPIFU")


query = """
select * from global_temp.RawPIFU
"""
df = spark.sql(query)
display(df)

# COMMAND ----------

df_processed_pifu = (df_raw_pifu
    .where ( F.col("EROC_DerMetricReportingName") == "Moved and Discharged")
    .where ( F.col("EROC_DerMonth") > '2021-03-01')
    .groupby(
        "EROC_DerMonth",
	    "EROC_DerProviderCode",
        "EROC_DerProviderName",
        "EROC_DerRegionName",
        "EROC_DerRegionCode",
        "EROC_DerICBCode",
        "EROC_DerICBName",
        "EROC_DerProviderAcuteStatus"
    )
    .agg( F.sum("EROC_Value").alias("Moved_or_Discharged"))
    .orderBy(
        "EROC_DerMonth",
        "EROC_DerProviderCode"
    )
)
       

#   --For ICB/Region filters, remove the filter for 'Acute' providers, and include the relevant ICB/Region fields for aggregation. 

display (df_processed_pifu)
