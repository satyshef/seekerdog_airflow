import requests
from bs4 import BeautifulSoup
import html2text
import re

class TelegramParser:
    def __init__(self, url):
        # если пост videonote заменяем адрес
        url = url.replace('https://telesco.pe', 'https://t.me')
        url = url + "?embed=1&mode=tme"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        self.url = url
        self.soup = soup

    def get_videonote(self):
        # url = url.replace('https://telesco.pe', 'https://t.me')
        # url = url + "?embed=1&mode=tme"
        # response = requests.get(url)
        # soup = BeautifulSoup(response.content, 'html.parser')
        video_links = self.get_video_links()
        return video_links[0]

    def parse_videonote_telesco(url):
        parts = url.split("/")
        id = "/" + parts[3] + "/" + parts[4]
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        container = soup.find('div', {'class': 'ts-post', 'data-url': id})
        if container == None:
           raise ValueError('Not find VideoNote container')
        video = container.find('video')
        src = video.find('source')['src']
        return src

    # Парсим веб версию поста
    # TODO: не работает если большое видео
    def get_video_links(self):
        #soup = BeautifulSoup(html, 'html.parser')
        videos = self.soup.find_all('video')
        result = []
        for v in videos:
            result.append(v['src'])
    
        return self.delete_double(result)


    def get_img_links(self):
        links = []
        for a_tag in self.soup.find_all('a', {'class': 'tgme_widget_message_photo_wrap'}):
            if a_tag:
                style_str = a_tag['style']
                # Ищем URL фотографии в атрибуте style
                photo_url = style_str.split("url('")[1].split("')")[0]
                links.append(photo_url)

        return links

    def get_text(self):
        tag = self.soup.find('div', {'class': 'tgme_widget_message_text js-message_text'})
        if tag != None:
           html = tag.decode_contents(formatter="html")
           return html2text.html2text(html)
           #return prepare_markdown(text)
        return ''


    # Удалить дубликаты в срезе
    def delete_double(self, in_list):
       result = []
       for i in in_list:
          if i not in result:
              result.append(i)

       return result
    

