# Databricks notebook source
from env import env
from src import utils, excel

import openpyxl
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

#Load PIFU data
df_raw_pifu = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(env["pifu_path"])

display(df_raw_pifu)

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

wb = openpyxl.load_workbook('report_template.xlsx')
ws_speciality = wb['PIFU | England & Specialty']

excel.insert_pandas_df_into_excel(
    df = df_tfc_pivot,
    ws = ws_speciality,
    header = True,
    startrow = 11,
    startcol = 2,
    index = False,
)

wb.save('outputs/PIFU_MI.xlsx')
