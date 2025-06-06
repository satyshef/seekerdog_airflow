import asyncio
from datetime import datetime, timedelta

from airflow import models
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowSkipException

#from es_collector.operators.es_operator import ESCollector
import es_collector.eslibs.es as Elastic

import parsers.tomatoes.tomatoes.parser as tomparser
import parsers.tomatoes.youtube.scraper as youtube

DAG_ID = "movie_parser"
MOVIE_INDEX = "movies"
INTERVAL = timedelta(minutes=60)
server = BaseHook.get_connection('elasticsearch_host2')
#es = ESCollector.ESNew(server)
es = Elastic.New(server)

def get_movie_id(url):
   return url.rsplit('/', 1)[-1]


@task.python
def get_movie_list():
    movie_list = tomparser.parse_movies_list()
    if len(movie_list) == 0:
        raise ValueError('Movie list not parsed')
    return movie_list

#@task(task_id="search_movie_in_elastic")
@task.python
def check_movies(index, movie_list):
    for movie_url in reversed(movie_list):
       movie_url = movie_url.strip()
       if movie_url == "":
           continue

       id = get_movie_id(movie_url)

       query = {
          "query": {
               "term": {
                      "_id": id
                }
           }
       }

       result = es.search(index=index, body=query)
       if len(result["hits"]["hits"]) != 0:
           continue
       info = get_movie_info(movie_url)
       if info["url_image"] == "null":
           continue
       return info

    raise AirflowSkipException


def get_movie_info(url):
    if url == None:
       raise ValueError("Empty movie URL")

    info = tomparser.parse_movies_info(url)
    ## Ищем трейлер на youtube
    loop = asyncio.new_event_loop()
    query = info["name"] + " trailer"
    res = loop.run_until_complete(youtube.search(query))
    if "videos" in res and len(res["videos"]) != 0:
        info["url_youtube"] = res["videos"][0]["link"].strip()

    print(info)
    return info

@task.python
def save_movie(index, movie): 
    id = movie["id"]
    del movie["id"]
    result = es.index(index=index, body=movie, id=id)
    print("Save Movie", result)


with models.DAG(
    DAG_ID,
    schedule=INTERVAL,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["parser", "movies"],
) as dag: 
    movie_list = get_movie_list()
    movie = check_movies(MOVIE_INDEX, movie_list)
    #movie = get_movie_info(movie_url)
    save_movie(MOVIE_INDEX, movie)


