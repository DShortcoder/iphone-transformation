# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDFs):
        pass

class AirpodsAfterIphoneTransformer(Transformer):
    
    def transform(self, inputDFs):
        # Customers who have brought Airpods after buying the iPhone
        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")
        transactionInputDF.show()

        window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")
        transformedDF = transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(window_spec)
        )
        print("Airpods after buying iPhone")
        transformedDF.orderBy('customer_id', 'transaction_date', 'product_name').show()
        
        transformedDF_filtered = transformedDF.filter(
            (col("product_name") == "iPhone") & (col("next_product_name") == "AirPods")
        )
        transformedDF_filtered.orderBy('customer_id', 'transaction_date', 'product_name').display()

        customerInputDF = inputDFs.get("customerInputDF")
        customerInputDF.show()
        
        # Apply broadcast join
        joinDF = customerInputDF.join(broadcast(transformedDF_filtered), 'customer_id')
        print("joined df")
        joinDF.show()
        return joinDF.select('customer_id', 'customer_name', 'location')

class OnlyAirpodsAndIphone(Transformer):
    def transform(self, inputDFs):
        '''Customer who have brought only iPhone and Airpods nothing else'''
        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")
        group_df = transactionInputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
        group_df.show()
        
        group_df_filter = group_df.filter(
            (array_contains(col('products'), 'AirPods')) & 
            (array_contains(col('products'), 'iPhone')) & 
            (size(col('products')) == 2)
        )
        print("only iphone and Airpods")
        group_df_filter.display()

        customerInputDF = inputDFs.get("customerInputDF")
        customerInputDF.show()
        
        joinDF = customerInputDF.join(broadcast(group_df_filter), 'customer_id')
        print("joined df")
        joinDF.show()
        return joinDF.select('customer_id', 'customer_name', 'location')

# COMMAND ----------

# from pyspark.sql.window import Window
# from pyspark.sql.functions import*
# from pyspark.sql.types import*
# class Transformer:
#     def __init__(self):
#         pass
#     def transform(self,inputDFs):
#         # self.inputDFs=inputDFs
#         pass

# class AirpodsAfterIphoneTransformer(Transformer):
    
#     def transform(self,inputDFs):
#          #Customers who have brougt Airpods after buying the iphone

#          transactionInputDF=inputDFs.get("transactionInputDF")

#          print("transactionInputDF in transform")

#          transactionInputDF.show()

#          window_spec=Window.partitionBy("customer_id").orderBy("transaction_date")
#          transformedDF=transactionInputDF.withColumn("next_product_name",lead("product_name").over(window_spec)
#                                                     )
#          print("Airpods after buying iphone")
         
#          transformedDF.orderBy('customer_id','transaction_date','product_name').show()
#          transformedDF_filtered=transformedDF.filter((col("product_name")=="iPhone") & (col("next_product_name")=="AirPods"))
#          transformedDF_filtered.orderBy('customer_id','transaction_date','product_name').display()

#          customerInputDF=inputDFs.get("customerInputDF")
#          customerInputDF.show()
#         #  joinDF=customerInputDF.join(transformedDF_filtered,'customer_id','inner')
#         #here we apply broadcast join 
#          joinDF=customerInputDF.join(broadcast(transformedDF_filtered),'customer_id')
#         #  joinDF.select('customer_id','customer_name','location')
#          print("joined df")
#          joinDF.show()
#          return joinDF.select('customer_id','customer_name','location')


# class OnlyAirpodsAndIphone(Transformer):
#     def transform(self,inputDFs):
#         '''Customer who have brought only iphone and Airpods nothing else'''
#         transactionInputDF=inputDFs.get("transactionInputDF")

#         print("transactionInputDF in transform")

#         group_df=transactionInputDF.groupBy("customer_id").agg(collect_set("product_name").alias("products"))
#         group_df.show()
#         group_df_filter=group_df.filter((col('products').contains('AirPods')) & (col('products').contains('iPhone')) & (col('products').size()==2))
#         group_df_filter.display()

#         customerInputDF=inputDFs.get("customerInputDF")
#         customerInputDF.show()
#         joinDF=customerInputDF.join(broadcast(group_df_filter),'customer_id')
#         #  joinDF.select('customer_id','customer_name','location')
#         print("joined df")
#         joinDF.show()
#         return joinDF.select('customer_id','customer_name','location')
    