'''
Created on Aug 22, 2017

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
    df = pd.read_excel('Online Retail.xlsx')
    
    df['Description'] = df['Description'].str.strip()
    df.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
    df['InvoiceNo'] = df['InvoiceNo'].astype('str')
    df = df[~df['InvoiceNo'].str.contains('C')]
    
#     df_filter = df[(df['Country'] == 'United Kingdom') & (df['InvoiceDate'] >= 6) & (df['InvoiceDate'].dt.month <= 9)]
    df_filter = df
    p = df_filter.pivot_table(index = 'InvoiceNo', columns = 'Description', values = 'Quantity', aggfunc=np.sum)
    p = p.fillna(0).applymap(encode_units)
    print(p)
    
    F_P = apriori(p, min_support=0.02, use_colnames=True)
    rule_p = association_rules(F_P, metric="lift", min_threshold=1)
    print(rule_p)
    