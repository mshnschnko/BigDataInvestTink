# Используем официальный образ Python 3.10 на базе slim
FROM python:3.10-slim

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Создаём рабочую директорию внутри контейнера
WORKDIR /app/for_rabbit

# Копируем только requirements.txt для установки зависимостей
COPY requirements.txt .

# Устанавливаем зависимости из requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код приложения
COPY from_parser_to_rabbitmq.py ./
COPY .env ./.env

# Определяем команду для запуска скрипта
CMD ["python", "from_parser_to_rabbitmq.py"]
