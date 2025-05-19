#import json
import re

# извлекаем номера телефонов из текста
def parse_phone_numbers(text):
    
    text = text.replace(" ", "")
    text = text.replace(" ", "")
    text = text.replace("-", "")
    text = text.replace("(", "")
    text = text.replace(")", "")
    # Шаблон для поиска номеров телефонов
    pattern = r'\+?\d{1,2}[-\s]?\(?\d{3}\)?[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}'
    phone_numbers = re.findall(pattern, text)

    # Заменяем номера, начинающиеся с 8, на номера, начинающиеся с +7
    formatted_numbers = []
    for number in phone_numbers:
        #formatted_number = re.sub(r'^\+?8', '+7', number)
        formatted_number = re.sub(r'^\+?[78]', '+7', number)
        formatted_numbers.append(formatted_number)

    return formatted_numbers

def parse_tglinks(text):
    links = []
    source_links = extract_tglinks(text)
    for link in source_links:

        link = link.lower()
        if link.startswith("@"):
            #link = link.replace("@", '')
            links.append(link)
            continue

        link = link.replace("https://", '')
        link = link.replace("http://", '')
        segments = link.split("/")

        if len(segments)<2:
            continue

        if segments[1].startswith("+") or len(segments[1]) < 5:
            continue
        
        links.append("@" + segments[1])

    return links


def extract_tglinks(text):
    pattern = r'https?://(?:t\.me/[\w/]+|@[\w_]+)|@[\w_]+'
    links = re.findall(pattern, text)
    return links

def extract_links(text):
    if text == "":
        return []
    #pattern = r'\[([^\]]+)\]\(([^)]+)\)|http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    pattern = r'\((http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)\)'


    links = re.findall(pattern, text)
    return links



def unique_list(lst):
    unique_list = []
    for item in lst:
        if item not in unique_list:
            unique_list.append(item)
    return unique_list