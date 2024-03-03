import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

def insert_author():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['author_id', 'author_name']
    copy_command = f"COPY {'author'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/authors.csv')
def insert_customer():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['customer_id', 'customer_name']
    copy_command = f"COPY {'customer'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/customers.csv')
def insert_seller():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['seller_id', 'seller_name']
    copy_command = f"COPY {'seller'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/sellers.csv')
def insert_category():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['category_id', 'category']
    copy_command = f"COPY {'category'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/categories.csv')
def insert_written():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['product_id', 'author_id']
    copy_command = f"COPY {'written'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/writtens.csv')
def insert_review():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['review_id', 'created_at', 'rating', 'title', 'content', 'thank_count', 'customer_id', 'product_id', 'seller_product_id']
    copy_command = f"COPY {'review'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/reviews.csv')
def insert_product():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')        
    columns = ['product_id', 'product_name', 'price', 'original_price', 'discount_rate', 'quantity_sold', 'rating_average', 'review_count', 'seller_id', 'category_id', 'seller_product_id']
    copy_command = f"COPY {'product'}({','.join(columns)}) FROM STDIN CSV HEADER"
    postgres_hook.copy_expert(copy_command, '/opt/airflow/csv/products.csv')