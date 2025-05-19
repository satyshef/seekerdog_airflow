# ACTUAL
import re
import requests

import es_collector.eslibs.webparser as webparser
import es_collector.eslibs.parser as parser

#====================================== Шаблоны ======================================    
# название чата(ссылка на сообщение) - имя отправителя - текст сообщения
def prepare_text_template1(project, source):
    if source["content"] == None:
        print("None content")
        return None

    post = source["content"].copy()

    if post['type'] == 'videonote':
        return None

    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    postLink = "[%s](%s)" % (chatName, source["content"]["link"])

    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        senderLink = f"[%s](%s)" % (senderName, senderLink)
    else:
        senderLink = ""

    post['text'] = "%s%s\n%s\n%s%s%s" % (
        project['before_post'],
        postLink,
        senderLink,
        project['before_text'],
        post['text'],
        get_after_text(project))
    return post


# изображение - ссылка на пост - текст
def prepare_post_chan_basic(project, source):
    if source["content"] == None:
        print("None content")
        return None
    
    post = source["content"].copy()
    
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
    
    author = "[%s](%s)" % (source["location"]["first_name"], source["content"]["link"])

    get_post_images(post)
    get_post_videos(post)
    post['text'] = "%s%s\n\n%s%s%s" % (
        project['before_post'], 
        author, 
        project['before_text'], 
        post['text'], 
        get_after_text(project))
    return post


# изображение - название чата  - текст
def prepare_post_chan_basic2(project, source):
    if source["content"] == None:
        print("None content")
        return None

    post = source["content"].copy()

    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post

    author = "*%s*" % (source["location"]["first_name"])

    get_post_images(post)
    get_post_videos(post)
    post['text'] = "%s%s\n\n%s%s%s" % (
        project['before_post'],
        author,
        project['before_text'],
        post['text'],
        get_after_text(project))
    return post



# изображение - текст - имя отправителя
def prepare_post_chan_second(project, source):
    if source["content"] == None:
        print("None content")
        return None
    
    post = source["content"].copy()
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
    
    #author = "_" + source["location"]["first_name"] + "_"
    author = "[%s](%s)" % (source["location"]["first_name"], source["content"]["link"])
    
    get_post_images(post)
    get_post_videos(post)
    post['text'] = "%s\n\n%s%s%s%s" % (
        project['before_post'], 
        project['before_text'], 
        post['text'], 
        get_after_text(project), 
        author)
    
    return post


# изображение - текст
def prepare_post_chan_clear(project, source):
    if source["content"] == None:
        print("None content")
        return None
    
    post = source["content"].copy()
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
    
    get_post_images(post)
    get_post_videos(post)
    post['text'] = "%s\n\n%s%s%s" % (
        project['before_post'], 
        project['before_text'], 
        post['text'], 
        get_after_text(project)
        )
    
    return post


def prepare_post_forward(source):
    if source["content"] == None:
        print("None content")
        return None
    post = source["content"].copy()
    
    if post['type'] == 'videonote':
        wp = webparser.TelegramParser(post['link'])
        post['video_link'] = wp.get_videonote()
        return post
    # FULL POST
    get_post_images(post)
    get_post_videos(post)
    #get_post_text(project, post)
    return post

#=====================================================================================  
def get_after_text(project):
    if "after_text" in project:
        return project['after_text']
    return "\n\n"

def generate_link(destination):
        if destination["type"] == "user":
            if destination["username"] != "":
                return "https://t.me/%s" % destination["username"]
            return ""
        # return "tg://user?id=%d" % destination["id"]
        else :
            if destination["username"] != "":
                return "https://t.me/%s" % destination["username"]
            return "https://t.me/c/%d" % destination["id"] 


def prepare_user(user, tags):
    user = {
        'first_name' : user['first_name'],
        'last_name' : user['last_name'],
        'username' : user['username'],
        'phone' : user['phone'],
        'tags' : tags,
        #'link' : source['content']['link']
        #source['location']['first_name']
    }
    return user

def prepare_markdown(text):
    if text == '':
        return ''
    text = text.strip()
    text = text.replace('**__', '_')
    text = text.replace('__**', '_')
    text = text.replace('__', '_')
    text = text.replace('**', '*')
    text = text.replace('[*', '[')
    text = text.replace('*]', ']')

    #Проверяем парность форматирующих символов, если не четное количество тогда все удаляем
    if text.count("*") % 2 != 0:
        text = text.replace('*', '')
    if text.count("_") % 2 != 0:
        text = text.replace('_', '')
    if text.count("~") % 2 != 0:
        text = text.replace('~', '')

    result = ''
    for i in range(len(text)):
        is_formating = is_formatting_char(text, i)
        if is_formating != None and is_formating == False:
            result += "\\"
        result += text[i]

    text = text.replace('\n', ' \n')
    return text.strip()

# Проверка, что символ окружен другими символами или находится в начале или конце строки
def is_formatting_char(text, index):
    if text[index] != '*' and text[index] != '_' and text[index] != '~':
        return None

    if index == 0 or index == len(text) - 1:
        return True
    
    if text[index - 1] == ' ' or text[index + 1] == ' ' or text[index - 1] == '\n' or text[index + 1] == '\n':
        return True
    
    return False

# предобработка текста
def handle_post_text(project, msg):
    if msg['content']['text'] == '':
        wp = webparser.TelegramParser(msg['content']['link'])
        msg['content']['text'] = wp.get_text()
        if msg['content']['text'] == '':
            # Тестовый вариант
            return False
            # Рабочий вариант
            #if msg['content']['type'] == 'text':
            #    return False
            #return True
    

    # проверяем количество ссылок в тексте
    if 'max_links' in project and project['max_links'] >= 0:
        links = parser.extract_links(msg["content"]["text"])
        #print("LINKS : ", len(links))
        if len(links) > project['max_links']:
            #print("MAX LINKS")
            return False
   
    
    msg['content']['text'] = prepare_markdown(msg['content']['text'])
    if "text_regex_patterns" in project and len(project["text_regex_patterns"]) > 0:
        apply_patterns(msg['content'], project["text_regex_patterns"])
    return True


def get_post_images(post):
    wp = webparser.TelegramParser(post['link'])
    links = wp.get_img_links()
    # если нет изображения, ищем ссылку в тексте
    if len(links) == 0:
        lnk = parser.extract_links(post['text'])
        if len(lnk) == 0:
            return
        links = []
        for link in lnk:
            if is_image_url(link):
                links.append(link)
        
    post["foto_link"] = links
    post["type"] = "photo"


def get_post_videos(post):
    wp = webparser.TelegramParser(post['link'])
    post["video_link"] = wp.get_video_links()


# пользовательские замены текста
# передаем словарь content чтобы изменения происходили внутри функции
def apply_patterns(post, patterns):
    if post['text'] != '':  
        for pattern, replace in patterns.items():
            post['text'] = re.sub(pattern, replace, post['text'])


# нужно решить в какой модуль ее перенести
def is_image_url(url):
    response = requests.head(url)
    content_type = response.headers.get('content-type')
    return content_type is not None and content_type.startswith('image/')


# ======================================= старые шаблоны под удаление ====================================

def prepare_demo1_post(source):
    if source["content"] == None:
        return None

    text = source["content"].get("text")
    if text == None:
        return None

    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return None

    #chatLink = generate_link(source["location"])
    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    #postLink = "[Сообщение](%s):\n" % source["content"]["link"]

    result = "*%s*\n" % (chatName)

    if source['sender']['id'] != source["location"]['id']:
        #senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"\n*%s*:\n" % (senderName)
    else:
        result += "\n\n"

    #result += postLink
    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)

    post = source["content"]
    post["type"] = "text"
    post["text"] = result
    return post


# название чата:имя отправителя:ссылка на сообщение:текст сообщения
def prepare_template1_post(source):
    if source["content"] == None:
        return None
    
    text = source["content"].get("text")
    if text == None:
        return None
    
    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return None
    
    chatLink = generate_link(source["location"])
    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    postLink = "[Сообщение](%s):\n" % source["content"]["link"]

    result = "[%s](%s)\n" % (chatName, chatLink)

    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += postLink
    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)
    
    post = source["content"].copy()
    post["type"] = "text"
    post["text"] = result
    return post


# название чата:имя отправителя:текст сообщения:ссылка на сообщение
def prepare_template2_post(source):
    if source["content"] == None:
        return None
    
    text = source["content"].get("text")
    if text == None:
        return None
    
    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return None
    
    chatLink = generate_link(source["location"])
    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)
    postLink = "[ссылка на пост](%s)" % source["content"]["link"]

    result = "[%s](%s)\n" % (chatName, chatLink)

    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)
    result += "\n\n%s" % postLink
    post = source["content"].copy()
    post["type"] = "text"
    post["text"] = result
    return post

# ссылка на сообщение:имя отправителя:текст сообщения
def prepare_template3_post(source):
    if source["content"] == None:
        return None

    text = source["content"].get("text")
    if text == None:
        return None

    text = prepare_markdown(text)
    if text == '':
        print('Empty text')
        return None

    chatName = source["location"]["first_name"]
    chatName = re.sub(r'\[|\]', '', chatName)

    result = "[%s](%s)\n" % (chatName, source["content"]["link"])
  
    if source['sender']['id'] != source["location"]['id']:
        senderLink = generate_link(source["sender"])
        senderName = source["sender"]["first_name"]+" "+source["sender"]["last_name"] 
        senderName = re.sub(r'\[|\]', '', senderName)
        result += f"[%s](%s)\n\n" % (senderName, senderLink)
    else:
        result += "\n\n"

    result += text
    #text +=  re.sub(r'\*|_|`|~', '', msg)

    post = source["content"].copy()
    post["type"] = "text"
    post["text"] = result
    return post


def prepare_forward_media(source):
    if source["content"] == None:
        print("None content")
        return None
    post = source["content"]
    parser = webparser.TelegramParser(post['link'])
    if post['type'] == 'videonote':
        post['video_link'] = parser.get_videonote()
        return post
        
    post["foto_link"] = parser.get_img_links()
    post["video_link"] = parser.get_video_links()
    post['text'] = ''
    return post



