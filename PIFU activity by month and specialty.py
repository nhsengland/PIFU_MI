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

df_tfc_pifu = (df_raw_pifu

  .where(F.col("EROC_DerMetricReportingName") == "Moved and Discharged") 
  .where(F.col("EROC_DerMonth") > "2021-03-01")   
  .groupBy(
    
    "RTT_Specialty_code",
    "RTT_Specialty_Description",
    "EROC_DerMonth" )  
  .agg(F.sum("EROC_Value").alias("Moved_or_Discharged") )  
  .orderBy("EROC_DerMonth", "RTT_Specialty_code") 
  .select("EROC_DerMonth", "RTT_Specialty_code", "RTT_Specialty_Description", "Moved_or_Discharged"   )  
)

display(df_tfc_pifu)

# COMMAND ----------


 
df_original=spark.sql("""SELECT 
       `EROC_DerMonth`
	  ,`RTT_Specialty_Code`
      ,`RTT_Specialty_Description`
       
	 --,`EROC_DerProviderAcuteStatus`
	 --`EROC_DerRegionCode`
     --,`EROC_DerRegionName`
     --,`EROC_DerICBCode`
	 -- ,`EROC_DerICBName`
      ,sum(`EROC_Value`) as `Moved_or_Discharged` 
  FROM `global_temp`.`RawPIFU`
  where `EROC_DerMetricReportingName` = 'Moved and Discharged'
  and   `EROC_DerMonth` >'2021-03-01'
  --For ICB/Region filters, remove the filter for 'Acute' providers, and include the relevant ICB/Region fields for aggregation. 
  group by 
       `RTT_Specialty_Code`
      ,`RTT_Specialty_Description`
      ,`EROC_DerMonth`  
	 order by `EROC_DerMonth`, RTT_Specialty_Code""")

# COMMAND ----------

utils.assert_spark_frame_equal(df_tfc_pifu, df_original)
