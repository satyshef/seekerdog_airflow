# Удаляет битые ссылки Телеграм
from datetime import datetime, timedelta

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook

import es_collector.eslibs.es as Elastic

DAG_ID = "delete_bad_links"
MESSAGE_INDEX = "tglink"
INTERVAL = timedelta(minutes=30)
SIZE = 1000
conn = BaseHook.get_connection('elasticsearch_host2')
es = Elastic.New(conn)


def elasticsearch_delete_tgmsg():
#    conn = BaseHook.get_connection('elasticsearch_host2')
#    es = ESCollector.ESNew(conn)

    #es_hook = ElasticsearchPythonHook(hosts=conn.host)
    #es = Elasticsearch([{'host': conn.host, 'port': conn.port, 'use_ssl': True}])
    query = {
              "size": SIZE,
              "query": {
                 "bool": {
                   "should": [
                              {
                                "match": { "status.state": "error:405" } 
                              },
                              {
                                "match": { "status.state": "error:406" } 
                              }
                   ]
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
    tags=["delete", "system", "tglink", "v2"],
) as dag:
    es_python = PythonOperator(
        task_id="delete_bad_tglink", 
	python_callable=elasticsearch_delete_tgmsg
    )

