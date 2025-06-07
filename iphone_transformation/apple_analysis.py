# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:
    '''ETL pipeline to generate the data for all customers who have bought Airpods just after buying the iPhone'''

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different source
        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #Step 2: Implement the transaction and  Apply transformation to find customers who bought Airpods after buying the iPhone
        firstTransformedDF = AirpodsAfterIphoneTransformer().transform(inputDFs)

        #Step 3: Load all required data from different sink
        AirPodsAfterIphoneLoader(firstTransformedDF).sink()


# FirstWorkFlow = FirstWorkFlow().runner()

# COMMAND ----------

class SecondWorkFlow:
    '''ETL pipeline to generate the data for all customers who have bought only iPhone and customers'''

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different source
        inputDFs=AirpodsAfterIphoneExtractor().extract()

        #Step 2: Implement the transaction and  Apply transformation to find customers who bought Airpods after buying the iPhone
        onlyAirpodsAndIphoneDF = OnlyAirpodsAndIphone().transform(inputDFs)

        #Step 3: Load all required data from different sink
        OnlyAirpodsAndIphoneLoader(onlyAirpodsAndIphoneDF).sink()


# SecondWorkFlow = SecondWorkFlow().runner()

# COMMAND ----------

class WorkFlowRunner:
    def __init__(self,name):
        self.name=name
    def runner(self):
        if self.name=="firstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name=="secondWorkFlow":
            return SecondWorkFlow().runner()

name="secondWorkFlow"

workFlowrunner=WorkFlowRunner(name).runner()

# COMMAND ----------

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()
# input_df=spark.read.format('csv')\
#         .option('header','true')\
#             .option('inferSchema','true')\
#                 .load('dbfs:/FileStore/tables/Customer_Updated.csv')
                
# input_df.show()

# COMMAND ----------

# customer_df=spark.read.format('delta').table('avops.my_schema.customer_updated')
# customer_df.show()