product_url = 'https://tiki.vn/api/personalish/v1/blocks/listings?'
product_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referrer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Guest-Token': 'EtZd0Tml8y4I3kxv5uonOhPsSXGR9bz6',
    'Content-Type': 'application/json',
    'Via': '1.1 google',
    'X-Content-Type-Options': 'nosniff'
}
product_params = {
    'limit': 40,
    'include': 'advertisement',
    'page': 1,
    'aggregations': 2,
    'trackity_id': 'd3a5d230-b5b5-b643-f1ab-eed0b6703194',
    'category': 8322,
    'page': 1,
    'urlKey': 'nha-sach-tiki'
}

sa_url = 'https://tiki.vn/api/v2/products/{}?'
sa_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referrer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'X-Guest-Token': 'dC1Xx9k2aj3EeWwPoIuySHRUshJDVTl7',
    'Content-Type': 'application/json; charset=utf-8',
    'Via': '1.1 google',
    'X-Content-Type-Options': 'nosniff'
}
sa_params = {
    'platform': 'web',
    'spid': 35191894,
    'version': 3
}

review_url = 'https://tiki.vn/api/v2/reviews?'
review_headers = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referrer': 'https://tiki.vn/nha-sach-tiki/c8322',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
    'X-Guest-Token': '1HdCx3rAeKhulntzMOLSjD9sIPXfYy8m',
    'Content-Type': 'application/json; charset=utf-8',
    'Via': 'kong/2.1.4, 1.1 google',
    'X-Content-Type-Options': 'nosniff'
}
review_params = {
    'limit': 20,
    'include': 'comments,contribute_info,attribute_vote_summary',
    'sort': 'score|desc,id|desc,stars|all',
    'page': 1,
    'spid': 187827005,
    'product_id': 187827003,
    'seller_id': 1
}



