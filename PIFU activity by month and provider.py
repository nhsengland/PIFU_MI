# Databricks notebook source
from env import env

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

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT 
# MAGIC        `EROC_DerMonth`
# MAGIC 	  ,`EROC_DerProviderCode`
# MAGIC       ,`EROC_DerProviderName`
# MAGIC       ,`EROC_DerRegionName`
# MAGIC       ,`EROC_DerRegionCode`
# MAGIC       ,`EROC_DerICBCode`
# MAGIC       ,`EROC_DerICBName`
# MAGIC       ,`EROC_DerProviderAcuteStatus`
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
# MAGIC        `EROC_DerProviderCode`
# MAGIC       ,`EROC_DerProviderName`
# MAGIC       ,`EROC_DerMonth`
# MAGIC       ,`EROC_DerRegionName`
# MAGIC       ,`EROC_DerRegionCode`
# MAGIC       ,`EROC_DerICBCode`
# MAGIC       ,`EROC_DerICBName`
# MAGIC       ,`EROC_DerProviderAcuteStatus` 
# MAGIC 	 order by EROC_DerMonth, EROC_DerProviderCode

# COMMAND ----------

df_processed_pifu = (df_raw_pifu
    .where ( F.col("EROC_DerMetricReportingName") == "Moved and Discharged")
    .where (F.col("EROC_DerMonth") > '2021-03-01')
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
    .agg (F.sum("EROC_Value").alias("Moved_or_Discharged"))
    .orderBy(
        "EROC_DerMonth",
        "EROC_DerProviderCode"
    )
)
       
# 	 --,`EROC_DerProviderAcuteStatus`
# 	 --`EROC_DerRegionCode`
#      --,`EROC_DerRegionName`
#      --,`EROC_DerICBCode`
# 	 -- ,`EROC_DerICBName`
#       ,sum(`EROC_Value`) as `Moved_or_Discharged` 
#   FROM `global_temp`.`RawPIFU`
#   where `EROC_DerMetricReportingName` = 'Moved and Discharged'
#   and   `EROC_DerMonth` >'2021-03-01'
#   --For ICB/Region filters, remove the filter for 'Acute' providers, and include the relevant ICB/Region fields for aggregation. 
#   group by 
#        `EROC_DerProviderCode`
#       ,`EROC_DerProviderName`
#       ,`EROC_DerMonth`
#       ,`EROC_DerRegionName`
#       ,`EROC_DerRegionCode`
#       ,`EROC_DerICBCode`
#       ,`EROC_DerICBName`
#       ,`EROC_DerProviderAcuteStatus` 
# 	 order by `EROC_DerMonth`
display (df_processed_pifu)
