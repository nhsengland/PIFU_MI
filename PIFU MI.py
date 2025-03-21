# Databricks notebook source
from env import env
from src import utils, excel

import openpyxl
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

#Load PIFU data
df_raw_pifu = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(env["pifu_path"])
wb = openpyxl.load_workbook('report_template.xlsx')
display(df_raw_pifu)

# COMMAND ----------

# MAGIC %md
# MAGIC #specialty

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

df_tfc_pivot = (df_tfc_pifu
                .groupBy("RTT_Specialty_code", "RTT_Specialty_Description")
.pivot("EROC_DerMonth")
.agg(F.sum("Moved_or_Discharged") )
.orderBy("RTT_Specialty_code")
)
display(df_tfc_pivot)

# COMMAND ----------

df_tfc_pivot = df_tfc_pivot.toPandas()

# COMMAND ----------


ws_speciality = wb['PIFU | England & Specialty']

excel.insert_pandas_df_into_excel(
    df = df_tfc_pivot,
    ws = ws_speciality,
    header = True,
    startrow = 11,
    startcol = 2,
    index = False,
)



# COMMAND ----------

# MAGIC %md
# MAGIC #month and org

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

# COMMAND ----------

#putting the data in pivot table format
df_provider_pivot = (df_processed_pifu
    .groupby(
        "EROC_DerRegionCode",
        "EROC_DerRegionName",
        "EROC_DerICBCode",
        "EROC_DerICBName",
        "EROC_DerProviderCode",
        "EROC_DerProviderName",
        "EROC_DerProviderAcuteStatus",
    )
    .pivot("EROC_DerMonth")
    .agg(F.sum("Moved_or_Discharged"))
    .orderBy(
        "EROC_DerRegionName",
        "EROC_DerICBCode",
        "EROC_DerProviderCode"
        )
)

display(df_provider_pivot)

# COMMAND ----------

#converting the pivot to pandas databframe

df_pd_provider_pivot = df_provider_pivot.toPandas()

# COMMAND ----------

#creating a workbook


ws_provider = wb['PIFU | By Org & Month']

excel.insert_pandas_df_into_excel(
    df = df_pd_provider_pivot,
    ws = wb['PIFU | By Org & Month'],
    header = True,
    startrow = 11,
    startcol = 2,
    index = False,
)



# COMMAND ----------

# MAGIC %md
# MAGIC #save report

# COMMAND ----------

# Save the workbook with the DataFrame inserted
wb.save('outputs/PIFU_MI.xlsx')
