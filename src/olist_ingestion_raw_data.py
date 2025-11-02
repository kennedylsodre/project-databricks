# Databricks notebook source
file_path = '/Volumes/raw_olist/raw_files/raw_files'

def ingestion_data(
    file_path: str, 
    catalog: str,
    schema: str):

    for file in dbutils.fs.ls(file_path):

        if file.name.endswith('.csv'): 
    
            table_name = f'raw_{file.name.replace('olist_','').replace('_dataset.csv','')}' 
            
            df = spark.read.format('csv').options(
                header='true', 
                inferSchema='true')\
                .load(file.path)
            
            print(f'Ingesting {file.name} into {table_name}')

            df.write.mode('overwrite').saveAsTable(f'{catalog}.{schema}.{table_name}') 

            print('Ingestão Concluída')

ingestion_data(file_path, 'raw_olist', 'raw_olist')


