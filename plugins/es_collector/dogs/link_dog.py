# Сбор пользователей из сообщений
#from airflow import DAG
from airflow.hooks.base_hook import BaseHook
import es_collector.operators.project_operators as Project
import es_collector.operators.tglink_operators as Link

server = BaseHook.get_connection('elasticsearch_host2')

def run_dag(dag, project):
    if dag == None or project == None:
        return

    succession_default(dag, project)

# Извлечение username. сценарий по умолчанию
def succession_default(dag, project):
    with dag:

        check = Project.check_actual(project)
        filter = Project.get_filter(server, project, check)
        messages = Project.get_messages(server, project, filter)
        links = Link.parse_messages(project, messages)
        result = Link.save_links(server, project, links)
        #check >> filter >> messages >> links >> result

