
# 🧭 Планировщик задач для системы мониторинга социальных сетей

## 🚀 Быстрый старт

### 🔐 Настройка доступа к GitHub по SSH

1. **Сгенерируй SSH-ключ:**

   ```bash
   ssh-keygen -t ed25519 -C "your_email@example.com"
   ```

2. **Скопируй публичный ключ:**

   ```bash
   cat ~/.ssh/id_ed25519.pub
   ```

3. **Добавь ключ в GitHub:**

   - Перейди: [https://github.com/settings/keys](https://github.com/settings/keys)
   - Нажми **"New SSH key"**
   - Вставь содержимое `.pub` файла

4. **Проверь соединение:**

   ```bash
   ssh -T git@github.com
   ```

5. **Измени origin-URL на SSH:**

   ```bash
   git remote set-url origin git@github.com:satyshef/seekerdog_airflow.git
   ```

6. **Клонируем репозиторий**:
    ```bash
    git clone git@github.com:satyshef/seekerdog_airflow.git
    ```

---

### 📦 Работа с подмодулями

#### Клонирование проекта с подмодулями:

```bash
git clone --recurse-submodules git@github.com:satyshef/es_collector.git
```

#### Если проект уже клонирован:

```bash
git submodule update --init --recursive
```

#### Push изменений:

```bash
git push origin main
```

---

## 📁 Структура проекта

### `projects/` — директория с конфигурациями парсинговых проектов

Используется внешними системами управления. Для корректного взаимодействия необходимо предоставить общий доступ к директории.

#### ⚙️ Настройка прав:

1. **Создай группу с фиксированным GID:**

   ```bash
   sudo groupadd -g 9999 seekerdog
   ```

2. **Назначь группу и установи права:**

   ```bash
   sudo chown -R :seekerdog ./projects
   sudo chmod -R 2775 ./projects
   sudo chmod g+s ./projects
   ```

3. **Пример настройки `docker-compose.yml`:**

   ```yaml
   services:
     container-a:
       image: some-image
       user: "1001:9999"
       volumes:
         - ./projects:/opt/airflow/projects

     container-b:
       image: other-image
       user: "1002:9999"
       volumes:
         - ./projects:/opt/airflow/projects
   ```

---

### `data/` — директория для хранения исходников **daga**

Используется системой генерации видеоконтента.

---

## ⚙️ Переменные окружения (`.env`)

```env
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW_CONN_ELASTICSEARCH_HOST2=http://login:password@host/http
```

- `AIRFLOW_UID` — UID пользователя для Airflow
- `_AIRFLOW_WWW_USER_USERNAME`, `_AIRFLOW_WWW_USER_PASSWORD` — задаются при первой инициализации БД
- `AIRFLOW_CONN_ELASTICSEARCH_HOST2` — параметры соединения с Elasticsearch по HTTP

---
