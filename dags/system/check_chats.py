from __future__ import annotations

import os
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic

DAG_ID = "check_chats"
CHATS_INDEX = "tgchats"
CONNECT_LIST_INDEX = "tgchats_connect_list"
INACTIVE_CHAT_TIME = "now-2h"
INTERVAL = timedelta(minutes=30)

conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)

@task.python
def get_lost_chats():
    
    query = {
                "_source": [], 
                "size": 100,
                "sort": {
                    "timestamp": {
                    "order": "asc"
                    }
                },
                "query": {
                    "bool": {
                    "must": [
                        {
                            "range": {
                            "timestamp": {
                                "lt": INACTIVE_CHAT_TIME
                            }
                            }
                        },
                        {
                        "match": {
                            "observable": "true"
                            }
                        }
                        ]
                    }
                }
    }
    result = es.search(index=CHATS_INDEX, body=query)
    print(result["hits"]["hits"])
    return result["hits"]["hits"]

@task.python
def return_to_connect_list(chats):
    for chat in chats:
        query = {
            "doc":{
                "status" : "wait",
            },
            "doc_as_upsert": "true"
        }
        address = chat['_source']['address']
        if address != '':
            result = es.update(index=CONNECT_LIST_INDEX, id=address, body=query)
            print(result)

with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["check", "chats"],
) as dag: 
    chats = get_lost_chats()
    return_to_connect_list(chats)
    
    


