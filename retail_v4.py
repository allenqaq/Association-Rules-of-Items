'''
Created on Aug 25, 2017

@author: allen
'''
import numpy as np
import pandas as pd
from mlxtend.preprocessing import OnehotTransactions
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession

def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1
    
if __name__ == '__main__':
    df = pd.read_excel('Online_Retail.xlsx')
#     df = pd.read_excel('Workbook1.xlsx')

    df['Description'] = df['Description'].str.strip()
    df.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
    df['InvoiceNo'] = df['InvoiceNo'].astype('str')
    df = df[~df['InvoiceNo'].str.contains('C')]
    df['CustomerID'] = df['CustomerID'].astype('str')
    df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%Y-%m-%d')
    
    df_filter1 = df[['InvoiceDate', 'CustomerID', 'Description', 'Quantity']]
    
    df_date_ID = df[['InvoiceDate', 'CustomerID']]
    df_date_ID = df_date_ID.drop_duplicates()
    
    list_add_all = []
    for i in df_date_ID.values:
        list_add1 = []
        list_add1.append(i[0])
        list_add1.append(i[1])
        list_add1.append(i[0])
        list_add1.append(1)
        list_add_all.append(list_add1)
        
        if i[1] != 'nan':
            list_add2 = []
            list_add2.append(i[0])
            list_add2.append(i[1])
            list_add2.append(i[1])
            list_add2.append(1)
            list_add_all.append(list_add2)
        
    df_add = pd.DataFrame(list_add_all, columns = ['InvoiceDate', 'CustomerID', 'Description', 'Quantity'])

    df2 = df_filter1.append(df_add, ignore_index=True)
    
    p = df2.pivot_table(index = ['InvoiceDate', 'CustomerID'], columns = 'Description', values = 'Quantity', aggfunc=np.sum)
    p = p.fillna(0).applymap(encode_units)
#     print(p)
#     p.to_csv('p.csv')
    oht = OnehotTransactions()
    oht.fit([list(p.columns)])
    transactions = oht.inverse_transform(p.values)

     
    spark = SparkSession.builder.appName("FPGrowthExample").getOrCreate()
         
    df3 = spark.createDataFrame([(value,) for value in transactions],["items"])
    fpGrowth = FPGrowth(itemsCol="items", minSupport=0.0075, minConfidence = 0.1)
#     print(df3)
    model = fpGrowth.fit(df3)
     
    freqItemsets = model.freqItemsets
    
    associationRules = model.associationRules
#     freqItemsets.show()
#     freqItemsets.toPandas().to_csv('freqItemsets_v4_0006_05.csv')   
#     associationRules.show()
    associationRules.toPandas().to_csv('output_retail_v4_00075_01.csv')   
