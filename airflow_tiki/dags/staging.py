import numpy as np
import pandas as pd
import sys
import datetime

def author_stg():
    df = pd.read_csv('/opt/airflow/raw_data/products.csv')
    df_new = pd.DataFrame({
        'author_id': range(1, df['author_name'].nunique() + 1),
        'author_name': df['author_name'].drop_duplicates().dropna()
    })
    df_new.to_csv('/opt/airflow/csv/authors.csv',index=None)

def customer_stg():
    df = pd.read_csv('/opt/airflow/raw_data/reviews.csv')
    df = df[['customer_id', 'customer_name']]

    # Bỏ trùng ID
    df_unique = df.drop_duplicates(subset='customer_id', keep='first')
    # Xử lý name null
    len_No_name = len(df_unique[df_unique["customer_name"].isnull() == True])
    replacement_name = ['Unnamed' + str(i) for i in range(len_No_name)]

    index_no_name, tmp = df_unique[df_unique["customer_name"].isnull() == True].index, 0
    for i in index_no_name:
        df_unique.loc[i, 'customer_name'] = replacement_name[tmp]
        tmp += 1  
    # df_unique['customer_name'].fillna(df_unique['customer_id'], inplace=True)
    df_unique.to_csv('/opt/airflow/csv/customers.csv',index=None)

def seller_stg():
    sellers = pd.read_csv('/opt/airflow/raw_data/products.csv')
    df = sellers[['seller_id', 'seller_name']]
    df_unique = df.drop_duplicates(subset='seller_id', keep='first')
    df_unique.at[1252, 'seller_name'] = 'seller_name'
    df_unique.to_csv('/opt/airflow/csv/sellers.csv',index=None)

def product_stg():
    df = pd.read_csv('/opt/airflow/raw_data/products.csv')
    df = df.rename(columns={'id': 'product_id'})
    df = df.rename(columns={'name': 'product_name'})
    df = df.rename(columns={'spid': 'seller_product_id'})
    df = df.drop(df[df['product_id'].duplicated() == True].index)
    df = df.drop(['seller_name', 'author_name'], axis=1)

    df_seller = pd.read_csv("/opt/airflow/csv/sellers.csv")
    df = df[df['seller_id'].isin(df_seller['seller_id'])]

    col = sorted(df['category'].unique())
    arr_index = list(range(1, len(col) + 1))
    new_data = pd.DataFrame({'category_id': arr_index, 'category': col})
    new_data.to_csv('/opt/airflow/csv/categories.csv', index=False)
    
    dictionary = dict(zip(col, arr_index))
    df['category'] = df['category'].map(dictionary)
    df = df.rename(columns={'category': 'category_id'})
    df.to_csv('/opt/airflow/csv/products.csv',index=False)


def review_stg():
    df = pd.read_csv('/opt/airflow/raw_data/reviews.csv')
    df = df.rename(columns={'id': 'review_id'})
    df = df.drop(['customer_name'], axis=1)
    df = df.drop(df[df.duplicated() == True].index)
    df = df.drop([561252])

    data_product = pd.read_csv("/opt/airflow/csv/products.csv")
    df = df[df['product_id'].isin(data_product['product_id'])]
    data_customer = pd.read_csv("/opt/airflow/csv/customers.csv")
    df = df[df['customer_id'].isin(data_customer['customer_id'])]
    df['created_at'] = df['created_at'].apply(lambda x: datetime.datetime.fromtimestamp(x))
    df.to_csv('/opt/airflow/csv/reviews.csv',index=None)

def written_stg():
    products= pd.read_csv('/opt/airflow/raw_data/products.csv')
    products = products.drop_duplicates(subset='id', keep='first')
    authors = pd.read_csv('/opt/airflow/csv/authors.csv')
    df = pd.merge(products, authors, on='author_name')
    df = df[['id', 'author_id']]
    df = df.rename(columns={'id': 'product_id'})
    data_product = pd.read_csv("/opt/airflow/csv/products.csv")
    df = df[df['product_id'].isin(data_product['product_id'])]

    data_author = pd.read_csv("/opt/airflow/csv/authors.csv")
    df = df[df['author_id'].isin(data_author['author_id'])]

    df.to_csv('/opt/airflow/csv/writtens.csv',index=None)