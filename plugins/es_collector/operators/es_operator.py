
# модуль взаімодействія с сервером Elasticsearch
import time
import json
from datetime import datetime, timedelta

import es_collector.eslibs.contented as Contented
import es_collector.eslibs.sender as Sender
import es_collector.eslibs.users as Users

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.decorators import task
from airflow import models
from airflow.exceptions import AirflowSkipException

from elasticsearch import Elasticsearch, exceptions

ADVANCE_WARNING = 24
#import telegram
#from telegram import InputMediaPhoto, InputMediaVideo

class ESCollector(BaseOperator):
    #server = ['server']

    @apply_defaults
    def __init__(self, host, port, *args, **kwargs):
        self.host = host
        self.port = port
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return


    @task.python
    def send_messages(server, project, messages, interval=1, check_user=False):
        bot_token = project["bot_token"]
        chat_id = project["chat_id"]
        if "disable_preview" in project:
            disable_preview = project["disable_preview"]
        else:
            disable_preview = True
        
        bot = Sender.TelegramWorker(bot_token)
        result = []
        for msg in messages:
            ESCollector.set_last_message(project, msg)
            #ESCollector.set_last_msg(server, project["filter_index"], project["filter_name"], msg["time"])
            # Проверяем использовали уже пользователя 
            if check_user:
                tags = '#' + project["name"]
                user_index = 'tgusers_' + project["name"]
                if ESCollector.save_user(server, user_index, msg['Sender'], tags) != True:
                    print('User Dont Save', msg['Sender'])
                    continue
           
            if project["post_template"] == 'template_1':
                post = Contented.prepare_template1_post(msg)
            elif project["post_template"] == 'template_2':
                post = Contented.prepare_template2_post(msg)
            elif project["post_template"] == 'template_3':
                post = Contented.prepare_template3_post(msg)
            elif project["post_template"] == 'template_4':
                post = Contented.prepare_template4_post(msg)
            elif project["post_template"] == 'demo_1':
                post = Contented.prepare_demo1_post(msg)
            elif project["post_template"] == 'forward_media':
                post = Contented.prepare_forward_media(msg)
            else:
                post = Contented.prepare_forward_post(msg)

            # Если post_type == 'information_1' и при этом текст отсутствует тогда post == None
            if post == None:
                print('Post is empty')
                continue
            
            #print("POST", post)
            text = ''
            if post['type']=='text':
                if 'text' in post:
                    text = post['text']
                else:
                    print('Text in text post not set')
                    continue
            

            for cid in chat_id:              
                if post['type']=='videonote':
                    print("SEND VIDEONOTE", post)
                    response = bot.send_videonote(cid, post)
                elif text !='':
                    print("SEND TEXT", text, " TO CHAT ", cid)
                    response = bot.send_text(cid, text, disable_preview)
                else:
                    print("SEND MEDIA", post)
                    response = bot.send_media_post(cid, post)

                time.sleep(interval)
#            print("MMMMMMMM", msg)
            ESCollector.save_message(server, project["project_index"], msg)
            result.append(msg)
                #if response.status_code != 200:
                #    print("SEND ERROR:", response)
                #    raise ValueError(response)

                #print("Response: ", response)
                #print(response.text)
                
        return result

    # Загружаем поисковый запрос пользователя
    @task(task_id="load_project_dont_used")
    def get_project(server, index, name):
      es = ESCollector.ESNew(server)
      query = {
          "query": {
                  "term": {
                      "_id": name
                  }
          }
      }
      result = es.search(index=index, body=query)
      if len(result["hits"]["hits"]) == 0:
          raise ValueError('Project %s not found' % name)

      params = result["hits"]["hits"][0]["_source"]
      print(params)
      return params 

        

    # Проверяем время актуальности задачи пользователя. Если задача не актуальна отправляем уведомление
    @task.python
    def date_checker(project):
        end_date = project['end_date']
        interval = project['interval']
        current_date = datetime.now()
        first_term = timedelta(hours=ADVANCE_WARNING)
        bot = Sender.TelegramWorker(project["bot_token"])
        start = current_date + first_term
        end = start + interval
        # Проверяем суточный остаток
        if start <= end_date and end > end_date:
            info = 'ℹ #info\n\nРабота парсера прекратится через %s\n\nДля продления услуги свяжитесь с @vagerman' % first_term
            for cid in project['chat_id']:
                response = bot.send_text(cid, info)
                print(response)
            raise AirflowSkipException

        # Проверяем окончание веремени
        if current_date <= end_date and (current_date+interval) > end_date:
            info = 'ℹ #info\n\nРабота парсера прекращена\n\nДля продления услуги свяжитесь с @vagerman'
            for cid in project['chat_id']:
                response = bot.send_text(cid, info)
                print(response)
            raise AirflowSkipException
        
        return True

    
    @task.python
    # Загружаем поисковый запрос пользователя
    # checked - результат проверки актуальности проекта. Нужен для того что бы дождаться результата date_ckecker
    def get_filter(server, project, checked):
      if checked != True:
          raise AirflowSkipException
      
      es = ESCollector.ESNew(server)
      query = {
          "query": {
                  "term": {
                      "_id": project["filter_name"]
                  }
          }
      }
      result = es.search(index=project["filter_index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          raise ValueError('Filter %s not found' % project["filter_name"])

      #print(result["hits"]["hits"][0]["_source"]["query"])
      filter = result["hits"]["hits"][0]["_source"]
      if "size" in project:
        filter["size"] = project["size"]
    
      if "search_after" in project:
        filter["search_after"]= [project["search_after"]]

      if "sort" in project:
        filter["sort"] = project["sort"]

      print(filter)
      return filter



    # Применяем пользовательский запрос
    @task.python
    def get_messages(server, project, query):
      es = ESCollector.ESNew(server)
      if query == None:
          raise ValueError("Empty Query")
      result = es.search(index=project["index"], body=query)
      if len(result["hits"]["hits"]) == 0:
          #raise ValueError('Messages %s not found' % project["filter_name"])
          print('Messages %s not found' % project["filter_name"])
          raise AirflowSkipException
      sources = result["hits"]["hits"]
      result = []
      for s in sources:
          if 'content' not in s['_source']:
              print('Empty content')
              continue
          post = s['_source']
          # Если поле text не существует то присваевыем ему ''
          if 'text' not in post['content']:
              post['content']['text'] = ''
          result.append(post)

      return result
    
    @task.python
    def dublicates_checker(server, project, messages):
        ##
        result = []
        for msg in messages:
            last_msg = msg
            if ESCollector.search_message(server, project["project_index"], msg, project["check_double_text"], project["check_double_user"]) == None:
                result.append(msg)
            else:
                print("Double", msg)
        #Если все дубли записываем время последнего сообщения в БД
        if len(result) == 0:
            if last_msg != None:
                ESCollector.set_last_message(project, last_msg)
                #ESCollector.set_last_msg(server, project["filter_index"], project["filter_name"], last_msg)
            raise AirflowSkipException
        return result

    @task.python
    def extract_users(messages):
        return Users.extract_users(messages)

    @task.python
    def save_list_to_file(filename, items):
        if items == None:
            raise "Empty user list"
        
        with open(filename, "w") as file:
            for item in items:
                file.write(item + "\n")
        return True
    
    @task.python
    def send_document(project, path):
        bot_token = project['bot_token']
        chat_id = project['chat_id']
        bot = Sender.TelegramWorker(bot_token)
        bot.send_file(chat_id, path, project['description'])

#=================================================================================================================================

    def set_last_message(project, msg):
        # Копируем обьект что бы не изменять оригинал
        p = project.copy()
        p["search_after"] = msg["time"]
        p['start_date'] = p['start_date'].strftime("%Y-%m-%d %H:%M:%S")
        p['end_date'] = p['end_date'].strftime("%Y-%m-%d %H:%M:%S")
        p['interval'] = int(p['interval'].total_seconds() / 60)

        with open(p["path"], "w") as file:
            del p["path"]
            del p["name"]
            del p["project_index"]

            json.dump(p, file, indent=4)

    # Write to es server. Dont used
    def set_last_msg_1(server, index, filter_id, msg_id):
        es = ESCollector.ESNew(server) 
        query = {
            "doc": {
                "search_after": [msg_id]
            }
        }
        result = es.update(index=index, id=filter_id ,body=query)
        if result['result'] != "updated":
            print("Set Message ID Error. Respone :", result, "\nMessage ID :", msg_id)
            raise ValueError('Message ID %s dont set' % msg_id)


    def save_user(server, index, user, tags): 
        es = ESCollector.ESNew(server) 
        #user = Contented.prepare_user(msg['_source']['Sender'], '#pankruxin')
        query = Contented.prepare_user(user, tags)
        try:
            result = es.create(index=index, id=user['id'] ,body=query)
            print("RESULT", result)
            return True
        except exceptions.ConflictError:
            return False
    
    def save_message(server, index, post): 
        es = ESCollector.ESNew(server) 
        #user = Contented.prepare_user(msg['_source']['Sender'], '#pankruxin')
        try:
            result = es.index(index=index, body=post)
            print("Save Post", result)
            return True
        except exceptions.ConflictError:
            return False


    # by_text - поиск текста
    # by_user - учитывать id пользователя (Sender.id)
    def search_message(server, index, message, by_text=True, by_user=True):
        must = []

        if by_text:
            text = message["content"]["text"]
            if text != None and text != '':
                q ={
                    "match_phrase": {
                        "content.text": text
                    }
                }
                must.append(q)

        if by_user:
            user_id = message["sender"]["id"]
            if user_id != None and user_id != '':
                q ={
                    "term": {
                        "sender.id": user_id
                    }
                }
                must.append(q)

        if len(must) == 0:
            return None      
        
        es = ESCollector.ESNew(server) 
        query = {
            "query": {
                "bool": {
                    "must": must
                }
            }
        }
         
        # Ищем документ в пользовательском индексе
        try:
          result = es.search(index=index, body=query)
        except exceptions.NotFoundError:
            return None

        if result == None or len(result["hits"]["hits"]) == 0:
            return None
        return result["hits"]["hits"][0]
        
    
    def ESNew(server):
        #extra = json.loads(server.extra)
        if server.login != "":
            es = Elasticsearch(
                hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
                ssl_show_warn=False,
                use_ssl = False,
                verify_certs = False,
                #ssl_assert_fingerprint=extra['fingerprint'],
                http_auth=(server.login, server.password)
            )
        else:
            es = Elasticsearch(
                hosts=[{"host": server.host, "port": server.port, "scheme": server.schema}],
                ssl_show_warn=False,
                use_ssl = False,
                verify_certs = False
            )
        
        print("ELASTIC")
        return es
