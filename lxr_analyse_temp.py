import datetime
import json
import numpy
import os
import requests
import scipy.stats
import time

import lxr_fetch


output_folder = './output'
output_file_template = output_folder + '/{}-{}.csv'
aggregate_file_template = output_folder + '/aggregated-' + time.strftime('%Y%m%d', time.localtime()) + '.csv'

def get_by_path(value, path):
    keys = path.split('.')
    result = value
    for key in keys:
        if not result:
            return result
        result = result.get(key)
    return result

def get_close_point(metric):
    return metric.get('close_point')

def get_market_value(metric):
    return metric.get('market_value')

def get_pe(metric):
    return get_by_path(metric, 'pe_ttm.weightedAvg')

def get_pb(metric):
    return get_by_path(metric, 'pb.weightedAvg')

def get_ps(metric):
    return get_by_path(metric, 'ps_ttm.weightedAvg')

def get_pes(metrics):
    return [get_pe(m) for m in metrics]

def get_pbs(metrics):
    return [get_pb(m) for m in metrics]

def get_pss(metrics):
    return [get_ps(m) for m in metrics]

def calc_temperature(sample):
    return map(lambda i: scipy.stats.norm.cdf(
        sample[i], numpy.mean(sample[:i + 1]), numpy.std(sample[:i + 1])) * 100, range(len(sample)))

def format_json_date(json_date):
    utc = datetime.datetime.strptime(metric['date'], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc)
    cst = utc.astimezone(tz=None)
    return cst.strftime('%Y-%m-%d')


if __name__ == '__main__':
    aggregated = []

    os.makedirs(output_folder, exist_ok=True)

    for stock_code, stock_code_desc in lxr_fetch.stock_codes.items():
        print('processing', stock_code, stock_code_desc)
        stock_code_name = stock_code_desc["name"]

        data_file_path = lxr_fetch.get_data_file_path(stock_code, stock_code_name)
        try:
            with open(data_file_path, 'r') as f:
                metrics = json.load(f)
        except FileNotFoundError:
            print('data file', data_file_path, 'not exists')
            continue

        metrics = [m for m in metrics if 'pe_ttm' in m and 'pb' in m]
        pes_temp = calc_temperature(get_pes(metrics))
        pbs_temp = calc_temperature(get_pbs(metrics))
        # pss_temp = calc_temperature(get_pss(metrics))

        with open(output_file_template.format(stock_code, stock_code_name), 'w') as f:
            f.write('date,close_point,market_value,pe,pb,pe_temp,pb_temp,(pe_temp+pb_temp)/2\n')
            for metric, pe_temp, pb_temp in zip(metrics, pes_temp, pbs_temp):
                f.write('{},{},{},{},{},{},{},{}\n'.format(
                    format_json_date(metric['date']), get_close_point(metric),
                    get_market_value(metric), get_pe(metric), get_pb(metric),
                    pe_temp, pb_temp, (pe_temp + pb_temp) / 2))
                last_metric, last_pe_temp, last_pb_temp = metric, pe_temp, pb_temp
        
        aggregated.append([
            format_json_date(last_metric['date']), stock_code, stock_code_name,
            get_close_point(last_metric), get_market_value(last_metric),
            get_pe(last_metric), get_pb(last_metric), get_ps(last_metric),
            last_pe_temp, last_pb_temp, (last_pe_temp + last_pb_temp) / 2,
        ])

    print('aggregate to file', aggregate_file_template)
    with open(aggregate_file_template, 'w') as f:
        f.write('date,stock_code,stock_code_name,close_point,market_value,pe,pb,ps,pe_temp,pb_temp,(pe_temp+pb_temp)/2\n')
        for a in aggregated:
            f.write(','.join(map(str, a)) + '\n')
