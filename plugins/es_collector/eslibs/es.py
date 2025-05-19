from elasticsearch import Elasticsearch, exceptions

def New(server):
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
    
    return es

def save_doc(es, index, post): 
    try:
        result = es.index(index=index, body=post)
        print("Save Doc", result)
        return True
    except exceptions.ConflictError:
        return False
    

# by_text - поиск текста
# by_user - учитывать id пользователя (Sender.id)
def search_message(es, index, message, by_text=True, by_user=True):
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
    


def search_link(es, index, link):
    if link == '':
        raise ValueError("Link dont set")
    
    must = [
        {
            "match": {
                "address": link
            }
        } 
    ]
    
    query = {
        "query": {
            "bool": {
                "must": must
            }
        }
    }
    
    try:
        result = es.search(index=index, body=query)
    except exceptions.NotFoundError:
        return None

    if result == None or len(result["hits"]["hits"]) == 0:
        return None
    return result["hits"]["hits"][0]
    
# Check dublicates
def is_dublicate(es, project, msg):
    if "check_double_text" in project or  "check_double_user" in project:
        if search_message(es, project["project_index"], msg, project["check_double_text"], project["check_double_user"]) != None:
            return True

    return False