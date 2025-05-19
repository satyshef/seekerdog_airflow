#from __future__ import annotations

#import os
from datetime import datetime, timedelta
#from elasticsearch import Elasticsearch

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic

DAG_ID = "check_connectors"
CONNECT_LIST_INDEX = "tgchats"
INACTIVE_TIME = "now-1m"
INTERVAL = timedelta(minutes=10)

#conn = BaseHook.get_connection('elasticsearch_host2')
#es = Elasticsearch([{'host': conn.host, 'port': conn.port, 'use_ssl': True}])

conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)

@task.python
def change_chat_status():
    
    query = {
        "query": {
            "bool": {
                "must": [
                        { 
                        "range": {
                                "time": {
                                    "lt": INACTIVE_TIME
                                }
                            }
                        },
                        {
                        "term": {
                            "status.state": "error:400"
                            }
                        }
                    ]
                }
        },
        "script": {
            "source": "ctx._source.status.state = 'ready'; ctx._source.time = 'now'"
        }
    }
    response = es.update_by_query(index=CONNECT_LIST_INDEX, body=query)
#    if response.status_code != 200 and response.status_code != 201:
#        raise ValueError(response)
    
    print("RESULT",response)

with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["check", "chats"],
) as dag: 
    change_chat_status()
    
    
    


