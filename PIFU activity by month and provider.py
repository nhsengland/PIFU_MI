# Databricks notebook source
from env import env
from src import utils, excel

import pandas as pd
import openpyxl

from pyspark.sql import functions as F

# COMMAND ----------

#Load PIFU data
df_raw_pifu = spark.read.option("header","true").option("recursiveFileLookup","true").parquet(env["pifu_path"])

display(df_raw_pifu)

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

wb = openpyxl.load_workbook('report_template.xlsx')

ws_provider = wb['PIFU | By Org & Month']

excel.insert_pandas_df_into_excel(
    df = df_pd_provider_pivot,
    ws = wb['PIFU | By Org & Month'],
    header = True,
    startrow = 11,
    startcol = 2,
    index = False,
)

# Save the workbook with the DataFrame inserted
wb.save('outputs/PIFU_MI.xlsx')
