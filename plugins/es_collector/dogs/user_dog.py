# Сбор пользователей из сообщений
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook


import es_collector.eslibs.es as Elastic
import es_collector.operators.project_operators as Project
import es_collector.operators.tgusr_operators as User
import es_collector.operators.os_operators as OS
     

DATA_DIR = '/opt/airflow/data/'
RESULTFILE_EXTENSION = 'csv'

server = BaseHook.get_connection('elasticsearch_host2')

def run_dag(dag, project):
    if dag == None or project == None:
        return
    if "succession" in project:
        succession = project["succession"]
    else:
        succession = "default"

    if succession == "extract_phones":
        succession_extract_phones(dag, project)
    else:
        succession_default(dag, project)
    


# Извлечение username. сценарий по умолчанию
def succession_default(dag, project):
    with dag: 
        result_file = get_filepath(project['name'])

        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages = Project.get_messages(server, project, filter)
        users = User.extract_usernames(messages)
        save_list = OS.save_list_to_file(result_file, users)
        send_document = User.send_file_with_description(project, result_file)
        #check >> filter >> messages >> users >> 
        save_list >> send_document


# Извлечение username+message. Из message парсятся номера телефонов
def succession_extract_phones(dag, project):
    with dag: 
        file_path = get_filepath(project['name'])

        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages = Project.get_messages(server, project, filter)
        result = User.extract_phone_messages(messages)
        save_list = OS.save_list_to_file(file_path, result)
        send_document = User.send_file_with_description(project, file_path)

        #check >> filter >> messages >> result >> 
        save_list >> send_document


# ======================== Service ===============================
def get_filepath(name):
    current_datetime = datetime.now() - timedelta(days=1)
    current_date_string = current_datetime.strftime('%Y-%m-%d')
    file_path = DATA_DIR + name + '_' + current_date_string + '.' + RESULTFILE_EXTENSION
    return file_path