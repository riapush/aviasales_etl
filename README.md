# Аналитический конвейер на базе Apache Airflow и Docker

Данный репозиторий содержит платформу для сбора, оркестрации и аналитической загрузки данных о ценах на авиабилеты, реализованную с использованием контейнеризированного микросервисного подхода.

## Структура проекта:
```
root/
├─ .env
├─ .gitignore
├─ docker-compose.yml
├─ README.md
├─ app/
│ ├─ Dockerfile
│ ├─ requirements.txt
│ └─ main.py
├─ airflow/
│ ├─ Dockerfile
│ ├─ requirements-airflow.txt
│ ├─ dags/
│ │ └─ ingest_to_mongo_dag.py
│ │ └─ mongo_to_postgres_dag.py
│ ├─ logs/
│ └─ plugins/
```

## Архитектура и компоненты

Проект развернут через **Docker Compose** и представляет собой стек из шести взаимосвязанных сервисов:

| Сервис | Технология | Роль |
| :--- | :--- | :--- |
| `air_quality_app` | FastAPI | Генерация данных. Имитирует внешнее API, генерируя записи о ценах и записывая их в MongoDB. |
| `mongodb` | Mongo 6 | Landing Zone. Хранение неструктурированных данных. |
| `postgres_analytics` | PostgreSQL 14 | Data Warehouse. Аналитическое хранилище структурированных данных (таблица `analytics.tickets`). |
| `Airflow Stack` | Apache Airflow | Оркестрация (ELT). Управляет DAGs для перемещения данных и администрирования. |
| `postgres` | PostgreSQL 14 | База метаданных Airflow. |
| `redis` | Redis 7 | Брокер сообщений для Celery Executor Airflow. |

## DAGs

В Airflow определены два ключевых конвейера, реализующих цикл Ingest & ELT:

1.  `ingest_to_mongo_dag`

      * Назначение: Периодический принудительный сбор данных.
      * Логика: Запускается по расписанию (`schedule_interval="0 * * * *"`, ежечасно). Выполняет HTTP POST-запрос к эндпоинту `/generate` сервиса `air_quality_app` для записи 60 новых записей в MongoDB.
      * Зависимость: При успехе триггерит выполнение DAG `mongo_to_postgres_elt`.

2.  `mongo_to_postgres_elt`

      * Назначение: Извлечение, загрузка и верификация данных в аналитическом хранилище.
      * Логика:
          * Extract: Извлекает записи из MongoDB, используя временное окно в 3 часа (`collected_ts >= NOW() - INTERVAL '3 hours'`).
          * Load: Загружает извлеченные данные в таблицу `analytics.tickets` в `postgres_analytics` с использованием UPSERT-логики (`ON CONFLICT (id) DO NOTHING`).
          * Verify: Проверяет общее количество записей, загруженных за последние 24 часа.

## Инструкция по запуску

Для развертывания платформы требуется установленный Docker и Docker Compose.

### Запуск и инициализация

Выполните команду:

```bash
docker-compose up -d --build
```

> Примечание: Для гарантии чистой инициализации базы данных (PostgreSQL и MongoDB), особенно после изменений в `.env` или предыдущих ошибок, используйте флаг `-v` (удаление томов): `docker-compose down -v && docker-compose up -d --build`.

Пример `.env`:
```
# Mongo
MONGO_INITDB_ROOT_USERNAME=admin
MONGO_INITDB_ROOT_PASSWORD=secret

# Postgres (Airflow Metadata)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
AIRFLOW_DB=airflow

# Postgres (Analytics)
ANALYTICS_DB_NAME=analytics
ANALYTICS_DB_USER=pguser
ANALYTICS_DB_PASSWORD=pgpass

# Airflow web creds
AIRFLOW_WWW_USER_USERNAME=admin
AIRFLOW_WWW_USER_PASSWORD=admin

# Airflow UID (Linux ID mapping)
AIRFLOW_UID=50000
```

### Доступ к интерфейсам

| Название | Порт | Адрес | Учетные данные |
| :--- | :--- | :--- | :--- |
| Airflow Webserver | 8080 | `http://localhost:8080` | admin / admin |
| FastAPI App | 8081 | `http://localhost:8081` | N/A |
| Postgres Analytics | 5433 | `localhost:5433` | pguser / pgpass |

### Активация конвейеров

После успешного запуска перейдите в веб-интерфейс Airflow (`localhost:8080`) и включите оба DAG: `ingest_to_mongo_dag` и `mongo_to_postgres_elt`. Сбор данных начнется согласно расписанию.
