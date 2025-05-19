import json
from datetime import datetime, timedelta

import es_collector.eslibs.sender as Sender

from airflow.decorators import task
from airflow.exceptions import AirflowSkipException

import es_collector.eslibs.es as Elastic

ADVANCE_WARNING = 24

# Проверяем время актуальности задачи пользователя. Если задача не актуальна отправляем уведомление
@task.python
def check_actual(project):
    end_date = project['end_date']
    interval = project['interval']
    current_date = datetime.now()
    first_term = timedelta(hours=ADVANCE_WARNING)
    bot = Sender.TelegramWorker(project["bot_token"])
    start = current_date + first_term
    end = start + interval
    # Проверяем суточный остаток
    if start <= end_date and end > end_date:
        if "send_notification" not in project or project['send_notification'] == True:
            if "notification1" not in project or project['notification1'] == "":
                info = 'ℹ #info\n\nРабота парсера прекратится через %s\n\nДля продления услуги свяжитесь с @vagerman' % first_term
            else:
                info = project['notification1']
            sendNotification(bot, project, info)
        raise AirflowSkipException
    # Проверяем окончание веремени
    if current_date <= end_date and (current_date+interval) > end_date:
        if "send_notification" not in project or project['send_notification'] == True:
            if "notification2" not in project or project['notification2'] == "":
                info = 'ℹ #info\n\nРабота парсера прекращена\n\nДля продления услуги свяжитесь с @vagerman'
            else:
                info = project['notification2']
            sendNotification(bot, project, info)
        raise AirflowSkipException
    return True


def sendNotification(bot, project, msg):
    if "notification_chat_id" in project:
        chats = project['notification_chat_id']
    else:
        chats = project['chat_id']
    for cid in chats:
        response = bot.send_text(cid, msg)
        print(response)


# Загружаем поисковый запрос пользователя
# checked ??? - результат проверки актуальности проекта. Нужен для того что бы дождаться результата date_ckecker
@task.python
def get_filter(server, project, checked):
    if checked != True:
        raise AirflowSkipException

    query = {
        "query": {
                "term": {
                    "_id": project["filter_name"]
                }
        }
    }
    
    es = Elastic.New(server)
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


# Загружаем сообщения проекта из ES
@task.python
def get_messages(server, project, query):
    if query == None:
        raise ValueError("Empty Query")
    es = Elastic.New(server)
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
