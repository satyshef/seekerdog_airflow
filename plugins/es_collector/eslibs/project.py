import json
from datetime import datetime, timezone

def save_last_message_time(project, msg):
    # Копируем обьект что бы не изменять оригинал
    p = project.copy()
    p["search_after"] = msg["time"]
    p['start_date'] = p['start_date'].strftime("%Y-%m-%d %H:%M:%S")
    p['end_date'] = p['end_date'].strftime("%Y-%m-%d %H:%M:%S")
    p['interval'] = int(p['interval'].total_seconds() / 60)

    with open(p["path"], "w", encoding='utf-8') as file:
        del p["path"]
        del p["name"]
        del p["project_index"]

        json.dump(p, file, indent=4, ensure_ascii=False)


def current_date():
    # Получение текущей даты и времени
    current_datetime = datetime.now(timezone.utc)
    # Форматирование даты в нужный формат
    formatted_date = current_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f%z")
    # Преобразование временной зоны в формат "+0000"
    return formatted_date[:-2] + ":" + formatted_date[-2:]