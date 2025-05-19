from airflow.decorators import task

import es_collector.eslibs.parser as Parser
import es_collector.eslibs.sender as Sender 

# Извлекаем usernames из сообщений
@task.python
def extract_usernames(messages):
    users = []
    for msg in messages:
        username = msg['sender']['username']
        if (username != '') and (username not in users):
            users.append(username)

    return users

@task.python
def extract_phone_messages(messages):
    result = []
    # Анализ каждого сообщения и извлечение номеров
    for m in messages:
        message = m['content']['text']
        message = message.strip()
        message = message.replace("\n", "\\n")

        phone_numbers = Parser.parse_phone_numbers(message)
        for number in phone_numbers:
            if number != message:
                #print(number)
                line = number + ";" + message
                result.append(line)
    
    return result


@task.python
def send_file_with_description(project, path):
    bot_token = project['bot_token']
    chat_id = project['chat_id']
    bot = Sender.TelegramWorker(bot_token)
    bot.send_file(chat_id, path, project['description'])