# Databricks notebook source
# MAGIC %run ../common/utility

# COMMAND ----------

# MAGIC %run ../schemas/salesforce_schemas

# COMMAND ----------

# MAGIC %run ../connectors/salesforce_connector

# COMMAND ----------

from pyspark.sql.functions import *
from simple_salesforce import Salesforce
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, DateType, BooleanType
from delta.tables import *
import pandas as pd

class SalesforceExtractor:
    
    _connection = None
    _schemas = None
    _util = None
    def __init__(self):
        self._connection = SalesforceConnector()
        self._schemas = SalesforceSchemas()
        self._util = Utility()

    # spark_dataframe.write => The write method returns a DataFrameWriter object, which allows you to specify various options and configurations for the write operation. 
    # .format("detla") => This specifies the format in which the data will be written.
    # .mode("overwrite") => if Delta table already exists at the location, its contents will be overwritten with the DataFrame. Delta table does not exist, it will be created.
    # .option("overwriteSchema", "true") => f the DataFrame's schema differs from the Delta table's schema, the Delta table's schema will be replaced with the DataFrame's schema.
    # .saveAsTable => This specifies the path or name of the Delta table where the data will be written.And saved to this Delta table.
    def write_delta_table(self,spark_dataframe, schema_name, table_name):
        spark_dataframe.write \
          .format("delta") \
          .mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(f'{schema_name}.{table_name}')


    def sync_salesforce_table(self, table_name, initial_load = False, drop_null_columns = False):
        is_table_exists = DeltaTable.isDeltaTable(spark, f'/mnt/datawarehouse/raw_salesforce/{table_name}')

        if is_table_exists:
            deltaTable = DeltaTable.forName(spark,f'raw_salesforce.{table_name}')  # This is a static method.used to create an instance of DeltaTable specific path where the Delta table is located.
            schema = StructType(deltaTable.toDF().schema)

        if initial_load:
            schema = None
            maxDate = '1999-01-01T00:00:00.000+0000'
        else:
            if table_name == 'opportunity_field_history':
                maxDate = spark.sql(f"Select max(CreatedDate) as MaxDate from raw_salesforce.{table_name}").first()['MaxDate']
            else:
                maxDate = spark.sql(f"Select max(SystemModstamp) as MaxDate from raw_salesforce.{table_name}").first()['MaxDate']
        
        query = self._schemas.get_query(table_name,maxDate)

        if query == None:
            return
        
        is_deleted_exists = 'IsDeleted' in query

        dfUpdates = self._connection.query_to_df(query, table_name, schema, drop_null_columns)

        if dfUpdates == None or dfUpdates==[]:
            return

        print(table_name+" -> Total Record Count:"+str(dfUpdates.count()))

        if initial_load:
            self.write_delta_table(dfUpdates, 'raw_salesforce', table_name)
            deltaTable = DeltaTable.forName(spark,f'raw_salesforce.{table_name}')
        else:
            deltaTable.alias(table_name).\
                merge(dfUpdates.alias('updates'),f'{table_name}.id = updates.id').\
                whenMatchedUpdate(set =self._schemas.get_fields_mapping(table_name)).\
                whenNotMatchedInsert(values =self._schemas.get_fields_mapping(table_name)).execute()
        
        if is_deleted_exists==True:
            deltaTable.delete("IsDeleted=True")        

# COMMAND ----------

il_param = dbutils.widgets.get('initial_load')
if il_param.lower() == 'true' or il_param.lower() == '1':
    initial_load = True
elif il_param.lower() == 'false' or il_param.lower() == '0':
    initial_load = False
else:
    raise Exception("Invalid Value for initial_load parameter, value should be in boolean")
try:
    se = SalesforceExtractor()
    se.sync_salesforce_table('account', initial_load)
    se.sync_salesforce_table('opportunity',True)
    se.sync_salesforce_table('user', initial_load)
    se.sync_salesforce_table('attribution', initial_load)
    se.sync_salesforce_table('event',True)
    se.sync_salesforce_table('forecasting_quota', initial_load)
    se.sync_salesforce_table('opportunity_split',True)
    se.sync_salesforce_table('opportunity_field_history', initial_load)
    se.sync_salesforce_table('quote_charge_detail', initial_load)
    se.sync_salesforce_table('subscription', initial_load)
    se.sync_salesforce_table('campaign', initial_load)
    se.sync_salesforce_table('quote', initial_load)
    se.sync_salesforce_table('product_rate_plan_charge', initial_load)
    se.sync_salesforce_table('record_type', initial_load)
    se.sync_salesforce_table('churn_reason', initial_load)
    se.sync_salesforce_table('contact', initial_load)
    se.sync_salesforce_table('user_role')	, initial_load	
    se.sync_salesforce_table('forecasting_item', initial_load)
    se.sync_salesforce_table('forecasting_type', initial_load)
    se.sync_salesforce_table('period', initial_load)
    se.sync_salesforce_table('task', initial_load)
    se.sync_salesforce_table('opportunity_split_type', initial_load)
    se.sync_salesforce_table('learning_session', initial_load)
    se.sync_salesforce_table('case', initial_load)
    se.sync_salesforce_table('zoom_logs', initial_load)
    se.sync_salesforce_table('sdr_quota', initial_load)
    se.sync_salesforce_table('zqu__quote__c', initial_load)
    se.sync_salesforce_table('quota_achievement_tier', initial_load)
    se._util.optimize_tables('raw_salesforce')
    dbutils.notebook.exit('Salesforce Extract Succeeded')
except e:
    print(f'Salesforce Extract Failed: {e}')
