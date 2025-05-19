import time 

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

from elasticsearch import exceptions

import es_collector.eslibs.project as Prolib  
import es_collector.eslibs.es as Eslib
import es_collector.eslibs.sender as Sender  
import es_collector.eslibs.es as Elastic


@task.python
def check_dublicates_movies(server, project, movies):
    es = Elastic.New(server)
    result = []
    for movie in movies:
        name = movie["_source"]["name"]
        query = {
          "query": {
               "match_phrase": {
                      "name": name
                }
           }
        }
        try:
            res = es.search(index=project["project_index"], body=query)
            if len(res["hits"]["hits"]) == 0:
                result.append(movie)
            else:
                Prolib.save_last_message_time(project, movie["_source"])
        except exceptions.NotFoundError:
            result.append(movie)

    if len(result) == 0:
        raise AirflowSkipException
    
    return result

@task.python
def get_movies(server, project, query):
      if query == None:
          raise ValueError("Empty Query")
      es = Elastic.New(server)
      result = es.search(index=project["index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          #raise ValueError('Messages %s not found' % project["filter_name"])
          print('Movies %s not found' % project["filter_name"])
          raise AirflowSkipException

      return result["hits"]["hits"]



@task.python
def prepare_messages(movies):
      # Проверить возможность избавиться от поля content
      result = []
      for m in movies:
          content = prepare_content(m)
          movie = {
             "content": {
                 "type": "text",
                 "text": content
             },
             "name": m["_source"]["name"],
             "sender": {"id": "movie_parser"},
             "time": m["_source"]["time"]
          }
          result.append(movie)
      #print(result)
      return result

@task.python
def send_messages(server, project, messages, interval=1):
        bot_token = project["bot_token"]
        chat_id = project["chat_id"]
        es = Elastic.New(server)
        
        if "disable_preview" in project:
            disable_preview = project["disable_preview"]
        else:
            disable_preview = True

        bot = Sender.TelegramWorker(bot_token)

        for msg in messages:
            Prolib.save_last_message_time(project, msg)
            for cid in chat_id:
               #text = Contented.prepare_markdown(msg["content"]["text"])
               text =msg["content"]["text"]
               res = bot.send_text(cid, text, disable_preview)
               print("Send result", res)
               time.sleep(interval)
            log = {
                "name": msg["name"],
                "time": Prolib.current_date(),
            }
            Eslib.save_doc(es, project["project_index"], log)


def prepare_content(movie):
      m = movie["_source"]
      info = ""
      for key, value in m["info"].items():
        info += "\n*" + key + "* : " + value
      
      content = "*" + m["name"] + "*" 
      content += "\n\n" + m["description"]
      if info != "":
        content += "\n" + info
      content += "\n\n[🍿](" + m["url_youtube"] + ")"
      return content
