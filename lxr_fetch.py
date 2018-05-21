import datetime
import json
import os
import requests
import shutil
import time


config_path = './config.json'

metrics_url = 'https://www.lixinger.com/api/open/a/indice/fundamental-info'
stock_codes = {
    '10000000905': { 'name': '中证500', 'source': 'http' },
    '10000000016': { 'name': '上证50', 'source': 'http' },
    '10000000300': { 'name': '沪深300', 'source': 'http' },
    '10000399673': { 'name': '创业板50', 'source': 'http' },
    '10000399006': { 'name': '创业板指', 'source': 'http' },
    '10000000015': { 'name': '红利指数', 'source': 'http' },
    '10000399903': { 'name': '中证100', 'source': 'http' },
    '10000000922': { 'name': '中证红利', 'source': 'http' },
    '10000000925': { 'name': '基本面50', 'source': 'http' },
    '10000399324': { 'name': '深证红利', 'source': 'http' },
    '10000399550': { 'name': '央视50', 'source': 'http' },
    '10000399005': { 'name': '中小板指', 'source': 'http' },
    '10000399330': { 'name': '深证100', 'source': 'http' },
    '10000000009': { 'name': '上证380', 'source': 'http' },
    '10000000010': { 'name': '上证180', 'source': 'http' },
    '10010000010001': { 'name': '恒生指数', 'source': 'http' },
    '10010000010002': { 'name': '国企指数', 'source': 'http' },
}

# 有些指数 api 暂时拿不到, 先从 http 拿
http_metrics_url = 'https://www.lixinger.com/api/analyt/stock-collection/price-metrics/load'

expected_metrics = ['pe_ttm', 'pb', 'ps_ttm', 'dividend_r', 'close_point', 'market_value']

data_folder = './data'
data_file_template = data_folder + '/{}-{}.json'

now = time.strftime('%Y%m%d%H%M%S', time.localtime())
five_years_ago = (datetime.datetime.now() - datetime.timedelta(days=365*5)).strftime('%Y-%m-%d')


def fetch_metrics(token, start_date, stock_code):
    payload = {
        'token': token,
        'startDate': start_date,
        'stockCodes': [stock_code],
        'metrics': expected_metrics,
    }
    r = requests.post(metrics_url, json=payload)
    return r.json()

def fetch_http_metrics(http_cookies, stock_code):
    payload = {
        'stockIds': [stock_code],
        'dateFlag': 'day',
        'granularity': 'y_10',
        'metricNames': ['pe_ttm', 'pb', 'ps_ttm', 'dividend_r', 'close_point', 'market_value'],
        'metricTypes': ['weightedAvg'],
    }
    r = requests.post(http_metrics_url, json=payload, cookies=http_cookies)
    return r.json()

def get_data_file_path(stock_code, stock_code_name):
    return data_file_template.format(stock_code, stock_code_name)

def download_metric(config, stock_code, stock_code_name, source):
    print('downloading', stock_code, stock_code_name)

    if source == 'http':
        incremental_metrics = fetch_http_metrics(config['http_cookies'], stock_code)
    else:
        incremental_metrics = fetch_metrics(config['token'], five_years_ago, stock_code)

    print('download success')
    if not incremental_metrics:
        print('[error] no data return, maybe something wrong')
        return

    incremental_metrics.sort(key=lambda e: e['date'])
    earliest_incremental_date = incremental_metrics[0]['date']

    data_file_path = get_data_file_path(stock_code, stock_code_name)
    backup_file_path = data_file_path + '.' + now

    inventory_metrics = []
    try:
        shutil.copyfile(data_file_path, backup_file_path)
        print(data_file_path, 'backuped to file', backup_file_path)
        with open(data_file_path, 'r') as f:
            inventory_metrics = json.load(f)
        print('load inventory metrics from', data_file_path)
    except FileNotFoundError:
        print('data file', data_file_path, 'not exists')
    
    inventory_metrics = list(filter(lambda e: e['date'] < earliest_incremental_date, inventory_metrics))
    inventory_metrics.extend(incremental_metrics)
    with open(data_file_path, 'w') as f:
        json.dump(inventory_metrics, f)
    print('save data to', data_file_path)


if __name__ == '__main__':
    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(data_folder, exist_ok=True)

    for stock_code, stock_code_desc in stock_codes.items():
        download_metric(config, stock_code, stock_code_desc['name'], stock_code_desc['source'])
