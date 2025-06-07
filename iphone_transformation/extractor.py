# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """Abstract class"""
    def __init__(self):
        pass
    def extract(self):
        pass
class AirpodsAfterIphoneExtractor(Extractor):
    def extract(self):
        '''Implement the steps for extracting or reading the data'''
        # Load transaction data from CSV
        transactionInputDF = get_data_source(
            data_type="csv",
            file_path='dbfs:/FileStore/tables/Transaction_Updated.csv'
        ).get_data_frame()

        # Order transaction data by customer_id and transaction_date
        transactionInputDF = transactionInputDF.orderBy("customer_id", "transaction_date")
        display(transactionInputDF)

        # Load customer data from Delta table
        customerInputDF = get_data_source(
            data_type="delta",
            file_path='avops.my_schema.customer_updated'
        ).get_data_frame()

        display(customerInputDF)

        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF
        }

        return inputDFs
