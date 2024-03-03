import requests
import time
import random
import pandas as pd
import json
import sys
# sys.path.append('/Docker/airflow_2')
import math

def parse_product(record): 
    d = dict()
    d['id'] = record.get('id')
    d['name'] = record.get('name')
    d['price'] = record.get('price')
    d['original_price'] = record.get('original_price')
    d['discount_rate'] = record.get('discount_rate')
    try: d['quantity_sold'] = record.get('quantity_sold').get('value')
    except: d['quantity_sold'] = 0
    d['rating_average'] = record.get('rating_average')
    d['review_count'] = record.get('review_count')
    d['seller_id'] = record.get('seller_id')
    d['category'] = record.get('visible_impression_info').get('amplitude').get('primary_category_name')
    d['spid'] = record.get('seller_product_id')
    return d

def fetch_products(headers, **kwargs):
    products = list()
    for i in range(1,2): ##########################################################
        kwargs['product_params']['page'] = i
        response = requests.get('https://tiki.vn/api/personalish/v1/blocks/listings?', headers=headers, params=kwargs['product_params'])
        if response.status_code == 200:
            # print('Request Success!: Page {}'.format(i))
            for record in response.json().get('data'):
                obj = parse_product(record)
                products.append(obj)

    df = pd.DataFrame(products)
    df.to_csv('/opt/airflow/raw_data/products.csv', index=False)

#------------
    
def fetch_sellers_authors(**kwargs):
    products = pd.read_csv('/opt/airflow/raw_data/products.csv')
    pid_list = products[['id', 'spid']]

    seller_list = list()
    author_list = list()
    i = 1
    count = 1
    
    for index, item in pid_list.iterrows():
        kwargs['sa_params']['spid'] = item[1]
        response = requests.get('https://tiki.vn/api/v2/products/{}?'.format(item[0]), headers=kwargs['sa_headers'], params=kwargs['sa_params'])
        time.sleep(0.4)
        if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
            print('Request: {}, PID: {} - SPID {}'.format(i,item[0],item[1]), count)
            current_seller = response.json().get('current_seller')
            if current_seller != None:
                seller_list.append(current_seller.get('name'))
            else: seller_list.append('')

            try: author = response.json().get('authors')[0].get('name')
            except: author = ''
            author_list.append(author)
        else: 
            seller_list.append('')
            author_list.append(author)
            count += 1
        i = i + 1    
    # return seller_list, author_list
        
    products['seller_name'] = seller_list
    products['author_name'] = author_list
    products.to_csv('/opt/airflow/raw_data/products.csv', index=False)

#------------
    
def parse_review(record): 
    d = dict()
    d['id'] = record.get('id')
    d['created_at'] = record.get('created_at')
    d['rating'] = record.get('rating')
    d['title'] = record.get('title')
    d['content'] = record.get('content')
    d['thank_count'] = record.get('thank_count')
    try: d['customer_name'] = record.get('created_by').get('full_name')
    except:d['customer_name'] = ''
    d['customer_id'] = record.get('customer_id')
    d['product_id'] = record.get('product_id')
    d['seller_product_id'] = record.get('spid')
    return d

def fetch_reviews(**kwargs):
    products = pd.read_csv('/opt/airflow/raw_data/products.csv')
    pid_list = products
    ii = 1
    tt = 0
    review_list = list()
    # try:
    for index, item in pid_list[['id', 'spid', 'seller_id', 'review_count']].iterrows():
        kwargs['review_params']['product_id'] = item[0]
        kwargs['review_params']['spid'] = item[1]
        kwargs['review_params']['seller_id'] = item[2]
        max_page = math.ceil(item[3]/20) # 5 is default limit in params, 20 is max.
        count = 0
        for i in range(1,max_page + 1):
            kwargs['review_params']['page'] = i
            response = requests.get('https://tiki.vn/api/v2/reviews?', headers=kwargs['review_headers'], params=kwargs['review_params'])
            time.sleep(0.2)
            if response.status_code == 200 and 'application/json' in response.headers['Content-Type']:
                count+=1
                reviews = response.json().get('data')
                for record in reviews:
                    obj = parse_review(record)
                    review_list.append(obj)
        if max_page!=count: tt+=1
        print('products', ii, tt)
        ii += 1
        
    # return review_list
    df = pd.DataFrame(review_list)
    df.to_csv('/opt/airflow/raw_data/reviews.csv', index=False)