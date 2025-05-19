from __future__ import annotations

import os
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic
#from es_collector_v2.operators.es_operator import ESCollector

DAG_ID = "delete_project_messages"
MESSAGE_INDEX = "project_*"
MESSAGE_AGE = "now-3d"
INTERVAL = timedelta(minutes=30)

conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)


def elasticsearch_delete_tgmsg():
#    conn = BaseHook.get_connection('elasticsearch_host2')
#    es = ESCollector.ESNew(conn)
    #es_hook = ElasticsearchPythonHook(hosts=conn.host)
    #es = Elasticsearch([{'host': conn.host, 'port': conn.port, 'use_ssl': True}])
    query = {
  	"query": {
    		"range": {
      			"time": {
        			"lt": MESSAGE_AGE
      			}
    		}
  	}
    }

    result = es.delete_by_query(index=MESSAGE_INDEX, body=query)
    print(result)
    return True

with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["delete", "elasticsearch", "project"],
) as dag:
    es_python = PythonOperator(
        task_id="send_delete_query", 
	python_callable=elasticsearch_delete_tgmsg
    )

