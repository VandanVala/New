# Databricks notebook source
# MAGIC %run ../common/utility

# COMMAND ----------

# MAGIC %run ../schemas/salesforce_schemas

# COMMAND ----------

# MAGIC %run ../connectors/salesforce_connector

# COMMAND ----------

#  def write_delta_table
    # spark_dataframe.write => The write method returns a DataFrameWriter object, which allows you to specify various options and configurations for the write operation. 
    # .format("detla") => This specifies the format in which the data will be written.
    # .mode("overwrite") => if Delta table already exists at the location, its contents will be overwritten with the DataFrame. Delta table does not exist, it will be created.
    # .option("overwriteSchema", "true") => f the DataFrame's schema differs from the Delta table's schema, the Delta table's schema will be replaced with the DataFrame's schema.
    # .saveAsTable => This specifies the path or name of the Delta table where the data will be written.And saved to this Delta table.

# def sync_salesforce_table

    # is_table_exists = DeltaTable.isDeltaTable(spark, f'/mnt/datawarehouse/raw_salesforce/{table_name}')
    # DeltaTable => This is a class provided by the Delta Lake library in Apache Spark. It contains methods for interacting with Delta tables.
    # .isDeltaTable => This is a static method of the DeltaTable class. It is used to determine whether the table at the specified path is a Delta table or not.
             #     => The isDeltaTable method returns a boolean value: 
    # spark => This is the SparkSession object that represents the entry point to programming Spark with the DataFrame API. 
        #  => It is required as the first argument to the isDeltaTable method, indicating the Spark session to use for the operation.

    # deltaTable = DeltaTable.forName(spark,f'raw_salesforce.{table_name}')
    # .forpath => This is a static method.used to create an instance of DeltaTable specific path where the Delta table is located.
    #
            