'''
Created on Aug 26, 2017

@author: allen
'''
import numpy as np
import pandas as pd
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules

def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1
    
if __name__ == '__main__':
#     df = pd.read_excel('Workbook1.xlsx')
    df = pd.read_excel('Online_Retail.xlsx')
    
    df['Description'] = df['Description'].str.strip()
    df.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
    df['InvoiceNo'] = df['InvoiceNo'].astype('str')
    df['CustomerID'] = df['CustomerID'].astype('str')
    df = df[~df['InvoiceNo'].str.contains('C')]
    
    df['InvoiceDate'] = df['InvoiceDate'].dt.strftime('%Y-%m')
    
    df_product = df[['InvoiceNo', 'Description', 'Quantity']]
    
    df_date_ID = df[['InvoiceNo', 'InvoiceDate', 'CustomerID']]
    df_date_ID = df_date_ID.drop_duplicates()
#     print(df_date_ID)
    
    list_add_all = []
    for i in df_date_ID.values:
        list_add1 = []
        list_add1.append(i[0])
        list_add1.append(i[1])
        list_add1.append(1)
        list_add_all.append(list_add1)
            
        if i[2] != 'nan':
            list_add2 = []
            list_add2.append(i[0])
            list_add2.append(i[2])
            list_add2.append(1)
            list_add_all.append(list_add2)
    
    df_add = pd.DataFrame(list_add_all, columns = ['InvoiceNo', 'Description', 'Quantity'])
    df_all = df_product.append(df_add, ignore_index=True)
#     print(df_all)
    
    p = df_all.pivot_table(index = ['InvoiceNo'], columns = 'Description', values = 'Quantity', aggfunc=np.sum)
    p = p.fillna(0).applymap(encode_units)
#     print(p)

    frequent_itemsets = apriori(p, min_support = 0.008, use_colnames = True)
#     print(frequent_itemsets)
    
    rules = association_rules(frequent_itemsets, metric="lift", min_threshold = 0.1)
#     print(rules)
     
    rules.to_csv('output_retail_v7_0008_01.csv')