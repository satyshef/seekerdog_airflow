# Скрипт сбора сообщений
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.decorators import task

import es_collector.operators.project_operators as Project
import es_collector.operators.tgmsg_operators as Message

# need delete
import re
import es_collector.eslibs.project as Prolib
from airflow.exceptions import AirflowSkipException
import es_collector.eslibs.contented as Contented


server = BaseHook.get_connection('elasticsearch_host2')
TIMEZONE = 3

def run_dag(dag, project):
    if dag == None or project == None:
        return
    if project["succession"] == "dubler":
        succession_dubler(dag, project)
    elif project["succession"] == "save2file":
        succession_save2file(dag, project)
    else:
        succession_default(dag, project)


def succession_default(dag, project):
    with dag:
        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages = Project.get_messages(server, project, filter)
        Message.send_messages(server, project, messages, 1)
        
def succession_dubler(dag, project):
    with dag:
        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages_all = Project.get_messages(server, project, filter)
        messages = Message.dublicates_checker(server, project, messages_all)
        Message.send_messages(server, project, messages, 1)

def succession_save2file(dag, project):
    with dag:
        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages_all = Project.get_messages(server, project, filter)
        messages = prepare_short_text(project, messages_all)
        save_messages_to_file(project['content']['file_name'], messages)

@task.python
def prepare_short_text(project, messages):
        
    if ('content' not in project) or ('max_text_len' not in project['content']):
        print('Nont set required parametr max_text_len')
        return None
    if 'min_text_len' not in project['content']:
        print('Nont set required parametr min_len')
        return None
    
    max_text_len = project['content']['max_text_len']
    min_text_len = project['content']['min_text_len']
    
    if 'lines_count' in project['content']:
        lines_count = project['content']['lines_count']
    else:
        lines_count = 7

    result = []
    for msg in messages:
        #print(msg['content']['text'])
        # Only text messages
        #if ('content' not in msg) or ('type' not in msg['content']) or msg['content']['type'] != 'text':
        #    print('NOT TEXT')
        #    continue

        msg['content']['text'] = msg['content']['text'].strip() 
        if msg['content']['text'] == '':
            continue
        if Contented.handle_post_text(project, msg) == False:
            continue
             
        text = msg['content']['text']
        text = process_string(text, min_text_len, max_text_len)
        if text != None:
            result.append(text)
            # если достигли нужного количества сообщений, сохраняем позицию последнего на elastic и прерываемся 
            if len(result) >= lines_count:
                Prolib.save_last_message_time(project, msg)
                if ('random' in project['content']) and (project['content']['random'] == True):
                    random.shuffle(result)
                if ('before_text' in project['content']) and (project['content']['before_text'] != ""):
                    result.insert(0, project['content']['before_text'])
                    #result.append(project['content']['before_text'])
                return result

    return None

@task.python
def save_messages_to_file(file_name, messages):
    if file_name == None or file_name == '':
        raise ValueError('File name is empty')
    
    if messages == None:
        print("Messages is None")
        raise AirflowSkipException

    file_name = generate_filename(file_name, 'txt')
    with open(file_name, 'w') as file:
        for item in messages:
            file.write(item + '\n')


# Подгонка строки до нужных размеров. Режется по обзацам
def process_string(text, min, max):
        text = clear_text(text)
        lines = text.split('\n')
        result = ''
        for line in lines:
            line = line.strip()
            if line == '':
                continue

            if result == '':
                tmp_result = line
            else:
                tmp_result = result + ' ' + line
            
            if len(tmp_result) > max:
                break

            result = tmp_result  

        if len(result) < min:
            return None
        
        if not result.endswith('.'):
            result += '.'

        return result.strip()


def clear_text(text):
    emoji_pattern = re.compile("["
                           u"\U0001F600-\U0001F64F"  # эмодзи обычных лиц
                           u"\U0001F300-\U0001F5FF"  # рисунки и символы
                           u"\U0001F680-\U0001F6FF"  # транспорт и символы
                           u"\U0001F1E0-\U0001F1FF"  # флаги стран
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=re.UNICODE)
    clean_text = emoji_pattern.sub(r'', text)
    clean_text = clean_text.replace('*', '')
    clean_text = clean_text.replace('__', '')
    return clean_text

# переместить в какую нибудь общую библиотеку            
def get_current_time(timezone):
    
    utc_now = datetime.utcnow()

    # Создайте объект timedelta для задания разницы в часах
    target_offset = timedelta(hours=timezone)

    # Примените разницу к текущей дате и времени
    current_datetime = utc_now + target_offset
    return current_datetime

def generate_filename(base, ext):
    current_datetime = get_current_time(TIMEZONE)
    result = base + '.' + current_datetime.strftime("%y%m%d%H%M%S") + "." + ext
    return result
