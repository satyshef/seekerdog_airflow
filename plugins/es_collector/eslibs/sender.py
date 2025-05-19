# ACTUAL
# Переименовать в telegram
import telebot
import requests

class TelegramWorker:
    def __init__(self, bot_token) -> None:
        self.bot =  telebot.TeleBot(bot_token, parse_mode='Markdown')


    def send_file(self, chat_id, path, description):
        try:
            # Открываем файл для чтения
            with open(path, 'rb') as file:
                self.bot.send_document(chat_id, file, caption=description, timeout = 60)
        except Exception as e:
             raise ValueError(f"Произошла ошибка: {e}")
            
    def send_text(self, chat_id, text, disable_preview=True):
        if len(text) > 4096:
           #разбиваем текст на части
           for x in range(0, len(text), 4096):
              result = self.bot.send_message(chat_id, text[x:x+4096], disable_web_page_preview=disable_preview, parse_mode='Markdown')
        else:
           result = self.bot.send_message(chat_id, text, disable_web_page_preview=disable_preview, parse_mode='Markdown', timeout = 60)
        return result

    def send_videonote(self, chat_id, post):
        url = post['video_link']
        if url == '':
            raise ValueError('Empty VideoNote link')
        #vid = telebot.types.InputVideoNote(url)
        #media.append(vid)
        data = requests.get(url).content
        #return None
        return self.bot.send_video_note(chat_id, data, timeout = 60)

    def send_media_post(self, chat_id, post):
        media = []
        for link in post['foto_link']:
            if is_remote_addr(link):
                response = requests.get(link)
                if response.status_code != 200:
                    continue
                image_file = response.content
            else:
                image_file = open(link, 'rb')
            img = telebot.types.InputMediaPhoto(image_file)
            media.append(img)

        for link in post['video_link']:
            if is_remote_addr(link):
                response = requests.get(link)
                if response.status_code != 200:
                    continue
                video_file = response.content
            else:
                video_file = open(link, 'rb')
            vid = telebot.types.InputMediaVideo(video_file)
            media.append(vid)

        text = post['text']
        #если медиа нет
        if len(media) == 0:
           if text != '':
               print("Send text instead of media")
               return self.send_text(chat_id, text)
           else:
               # Сообщение изначально без текста и оно либо удалено либо большой размер файла(невозможно загрузить медиа через веб) 
               print('Media dont send. Empty content')
               return None
               #raise ValueError('Media dont send. Empty content')
           
        if len(text) > 1024:
            #Если длинный текст отправляем медиа отдельно от текстста
            #media[0].caption = "%s" % (post["link"])
            self.bot.send_media_group(chat_id, media)
            self.send_text(chat_id, text)

        else:
            media[0].caption = text
            media[0].parse_mode = 'Markdown'
            return self.bot.send_media_group(chat_id, media, timeout = 60)


def is_remote_addr(url):
    return url.startswith('http://') or url.startswith('https://')
