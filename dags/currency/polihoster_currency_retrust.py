from datetime import datetime, timedelta

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowSkipException

import es_collector.eslibs.sender as Sender
import es_collector.eslibs.es as Elastic


BOT_TOKEN = "6078932856:AAHyPSOhwkUsCFW9Zw5v7y-sInZ2LH5a0sE"
CID = "-1002115235640"
DAG_ID = "polihoster_currency_retrust"
INDEX = "currency"
INTERVAL = timedelta(minutes=1440)
conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)
bot = Sender.TelegramWorker(BOT_TOKEN)

@task.python
def get_currency(index, name):
    query = {
          "size": 1,
          "sort": [
            {
              "time": {
                "order": "desc"
              }
            }
          ],
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "quote": name
                  }
                },
                {
                  "term": {
                    "base": "ton"
                  }
                },
                {
                  "range": {
                    "time": {
                      "gte": "now/d",
                      "lte": "now"
                    }
                  }
                }
              ]
            }
          }
        }


    result = es.search(index=index, body=query)
    if len(result["hits"]["hits"]) == 0:
        raise AirflowSkipException
        #raise ValueError('Empty currency list')
        
    print(result["hits"]["hits"][0]["_source"])
    return result["hits"]["hits"][0]["_source"]["value"]


@task.python
def generate_post(curTONRUB, curTONUAH):
     buy = round(curTONRUB / curTONUAH * 0.93, 2)
     sale = round(curTONRUB / curTONUAH * 1.07, 2)
     exampleUAH = buy * 1000
     exampleRUB = 1000 / sale
     text = "*Курс гривны на сегодня 🇺🇦 \n\nПокупка   %.2f\nПродажа  %.2f\n\n*1000 грн = %d руб\n1000 руб = %d грн\n\n[Онлайн обмен](https://t.me/mak7eron)\n\n[reTrust ® Финанс](https://t.me/retrust)    #promo" % (buy, sale, exampleUAH, exampleRUB)
     post = {
              "text": text,
              "link": "none",
              "foto_link": ["/opt/airflow/data/images/retrust_cover2.jpg"],
              "video_link": []
     }
     #https://t3.ftcdn.net/jpg/00/86/56/12/360_F_86561234_8HJdzg2iBlPap18K38mbyetKfdw1oNrm.jpg
     #/opt/airflow/dags/data/images/currency_1.jpg
     print(post)
     return post


@task.python
def send_post(bot, cid, post):
     bot.send_media_post(cid, post)


with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 11, 18, 6, 0, 0),
    catchup=False,
    tags=["currency", "polihoster", "retrust"],
) as dag: 
    curTONRUB = get_currency(INDEX, "rub")
    curTONUAH = get_currency(INDEX, "uah")
    post = generate_post(curTONRUB, curTONUAH)
    send_post(bot, CID, post)
