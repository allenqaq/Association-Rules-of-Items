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
#     df = pd.read_excel('Online_Retail.xlsx')
    df = pd.read_excel('Workbook1.xlsx')

    df['Description'] = df['Description'].str.strip()
    df.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
    df['InvoiceNo'] = df['InvoiceNo'].astype('str')
    df = df[~df['InvoiceNo'].str.contains('C')]
    df['CustomerID'] = df['CustomerID'].astype('str')
    df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%Y-%m-%d')
    
    df_filter1 = df[['InvoiceDate', 'CustomerID', 'Description', 'Quantity']]
    
    df_date_ID = df[['InvoiceDate', 'CustomerID']]
#     print(df_date_ID)
    
    for i in df_date_ID.values:
        print(i)
        if i[1] == 'nan':
            print('get!')
    
    df_date_ID = df_date_ID.drop_duplicates()
    
    