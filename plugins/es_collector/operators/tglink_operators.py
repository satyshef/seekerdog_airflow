
from airflow.decorators import task
#from airflow.exceptions import AirflowSkipException

import es_collector.eslibs.parser as Parser
import es_collector.eslibs.project as Prolib
import es_collector.eslibs.es as Elastic
import es_collector.eslibs.es as Eslib

@task.python
def parse_messages(project, messages):
    links = []
    for msg in messages:
        Prolib.save_last_message_time(project, msg)
        links += Parser.parse_tglinks(msg['content']['text'])

    links = Parser.unique_list(links)
 
    for link in links:
        if link.endswith("bot"):
            links.remove(link)

    return links


@task.python
def save_links(server, project, links):
    index = project["link_index"]
    if index == '':
        raise ValueError('Links Index dont set')
    es = Elastic.New(server)
    for link in links:
        if Eslib.search_link(es, index, link) != None:
            continue

        lnk = {
            "address": link,
        }
        Eslib.save_doc(es, index, lnk)
