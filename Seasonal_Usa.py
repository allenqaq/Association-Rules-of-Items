'''
Created on Jul 12, 2017

@author: allen
'''
import pandas as pd
import numpy as np
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import datetime
def encode_units(x):
    if x <= 0:
        return 0
    if x >= 1:
        return 1

if __name__ == '__main__':
    df = pd.read_excel('workbook1.xlsx')
    
    df['Description'] = df['Description'].str.strip()
    df.dropna(axis=0, subset=['InvoiceNo'], inplace=True)
    df['InvoiceNo'] = df['InvoiceNo'].astype('str')
    df = df[~df['InvoiceNo'].str.contains('C')]
    
#     print(df[(df['InvoiceDate'].dt.month < 11) & (df['Country'] == 'United Kingdom')])
    
#     df_filter = df[df['Country'] == 'United Kingdom' & df['InvoiceDate'] >= '' & df['InvoiceDate'] <= '']
     
    p = df.pivot_table(index = 'InvoiceNo', columns = 'Description', values = 'Quantity', aggfunc=np.sum)
    print(p)
    p = p.fillna(0).applymap(encode_units)
    print(p)

    F_P = apriori(p, min_support=0.03, use_colnames=True)
    print(F_P)
    rule_p = association_rules(F_P, metric="lift", min_threshold=1)
    print(rule_p)
