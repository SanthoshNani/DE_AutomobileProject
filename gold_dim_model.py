# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag', '0')

# COMMAND ----------

# getting the variable
incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION Model

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch relative columns

# COMMAND ----------

df_src = spark.sql('''
    SELECT DISTINCT(Model_ID) AS Model_ID, model_category
    FROM PARQUET.`abfss://silver@dataengcarsdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### dim_model sink - initial and incremental
# MAGIC
# MAGIC ##### This will check if the dim_table already exists or not

# COMMAND ----------


#check if the dim_model table already exists in catalog
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    df_sink = spark.sql('''
    SELECT dim_model_key, Model_ID, model_category
    FROM cars_catalog.gold.dim_model
    ''')


else:
    df_sink = spark.sql('''
    SELECT 1 as dim_model_key, Model_ID, model_category
    FROM PARQUET.`abfss://silver@dataengcarsdatalake.dfs.core.windows.net/carsales`
    WHERE 1=0
    ''')



# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Model_ID']==df_sink['Model_ID'], 'left')\
    .select(df_src['Model_ID'], df_src['model_category'], df_sink['dim_model_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_ID'], df_src['model_category'])

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate key

# COMMAND ----------

# MAGIC %md
# MAGIC **Fetch the max surrogate key from existing table**

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql("SELECT max(dim_model_key) FROM cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0] + 1

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Surrogate key column and ADD the max Surrogate key**

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - df_filter_old + df_filter_new (union)

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)
df_final.display()

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

# Incremental RUN
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@dataengcarsdatalake.dfs.core.windows.net/dim_model')
    # merge target with source
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key') \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
#Initial RUN
else:
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path', 'abfss://gold@dataengcarsdatalake.dfs.core.windows.net/dim_model')\
            .saveAsTable('cars_catalog.gold.dim_model')

            

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cars_catalog.gold.dim_model

# COMMAND ----------

