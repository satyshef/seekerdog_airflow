import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowSkipException

import es_collector.eslibs.es as Elastic

URL = "https://www.bestchange.ru/sberbank-to-ton.html"
DAG_ID = "bestchange_rub_ton"
INDEX = "currency"
INTERVAL = timedelta(minutes=60)
conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)


@task.python
def get_body(url):
    headers = {
        'authority': 'www.bestchange.ru',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'cookie': '_ym_uid=1643887200554385511; PHPSESSID=ahsb2fmv49cdf919tmmts61u3v; userid=4f067599a1be2b0e6a66cd473f13d077; pixel=1; time_offset=-180; _gid=GA1.2.1355483328.1695162500; _ym_d=1695162501; _ym_isad=2; _ga_DV11TBJYB2=GS1.1.1695162500.1.1.1695163359.60.0.0; _ga=GA1.2.1042334818.1695162500',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        if response.text == "":
            raise ValueError('Empty data from %s' % url)

        return response.text

    else:
        raise ValueError('Error load data from %s : %d' % (url, response.status_code))
        


@task.python
def parse_currency(html):
    soup = BeautifulSoup(html, "html.parser")
    element = soup.find('div', id = 'content_rates').find('div', class_ = 'fs')
    value = element.get_text(strip=False)
    values = value.split(" ");
    if len(values) < 3:
        raise ValueError('Error parse currency : %s' % value)
    return round(float(values[0]), 2)


@task.python
def save_currency(index, base, quote, value):
    data = {
        "base": base,
        "quote": quote,
        "value": value,
        "service": "bestchange"
    } 
    Elastic.save_doc(es, index, data)


with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 9, 22, 6, 40, 0),
    catchup=False,
    tags=["parser", "currency", "bestchange", "ton", "rub"],
) as dag: 
    html = get_body(URL)
    value = parse_currency(html)
    save_currency(INDEX, "ton", "rub", value)
