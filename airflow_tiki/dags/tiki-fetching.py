import pandas as pd
import numpy as np
import requests
import time
import random
import pandas as pd
import json
import sys
import os
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from fetch_data import fetching, parameters

default_args = {
    'start_date': pendulum.datetime(2024,2,27,tz='UTC'),
    'owner': 'airflow'
}

dag = DAG('tiki-fetching', default_args=default_args, schedule_interval='@once',catchup=False)

# crawl_products = PythonOperator(
#     task_id='crawl_products',
#     python_callable=fetching.fetch_products,
#     op_kwargs= {
#         'headers': parameters.product_headers,
#         'product_params': parameters.product_params,
#     },
#     dag=dag
# )

# crawl_sellers_authors = PythonOperator(
#     task_id='crawl_sellers_authors',
#     python_callable=fetching.fetch_sellers_authors,
#     op_kwargs= {
#         'sa_headers': parameters.sa_headers,
#         'sa_params': parameters.sa_params,
#     },
#     dag=dag
# )

crawl_reviews = PythonOperator(
    task_id='crawl_reviews',
    python_callable=fetching.fetch_reviews,
    op_kwargs= {
        'review_headers': parameters.review_headers,
        'review_params': parameters.review_params,
    },
    dag=dag
)

# crawl_products >> crawl_sellers_authors
# crawl_sellers_authors
crawl_reviews