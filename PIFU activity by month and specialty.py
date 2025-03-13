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



# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT 
# MAGIC        `EROC_DerMonth`
# MAGIC 	  ,`RTT_Specialty_Code`
# MAGIC       ,`RTT_Specialty_Description`
# MAGIC        
# MAGIC 	 --,`EROC_DerProviderAcuteStatus`
# MAGIC 	 --`EROC_DerRegionCode`
# MAGIC      --,`EROC_DerRegionName`
# MAGIC      --,`EROC_DerICBCode`
# MAGIC 	 -- ,`EROC_DerICBName`
# MAGIC       ,sum(`EROC_Value`) as `Moved_or_Discharged` 
# MAGIC   FROM `global_temp`.`RawPIFU`
# MAGIC   where `EROC_DerMetricReportingName` = 'Moved and Discharged'
# MAGIC   and   `EROC_DerMonth` >'2021-03-01'
# MAGIC   --For ICB/Region filters, remove the filter for 'Acute' providers, and include the relevant ICB/Region fields for aggregation. 
# MAGIC   group by 
# MAGIC        `RTT_Specialty_Code`
# MAGIC       ,`RTT_Specialty_Description`
# MAGIC       ,`EROC_DerMonth`  
# MAGIC 	 order by `EROC_DerMonth`
