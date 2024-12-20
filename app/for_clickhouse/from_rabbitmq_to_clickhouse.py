import os
import pika
import json
import logging
from datetime import datetime
import asyncio
from aio_pika import connect_robust, Message, DeliveryMode
from aio_pika.exceptions import AMQPConnectionError, ChannelInvalidStateError
from clickhouse_driver import Client as ClickHouseClient
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройки RabbitMQ
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')  # По умолчанию 'rabbitmq' для Docker-сети
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))  # По умолчанию порт RabbitMQ
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'rmuser')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'rmpassword')

# Настройки ClickHouse
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')  # По умолчанию 'clickhouse' для Docker-сети
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9000))  # По умолчанию порт ClickHouse
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', 'default')

# Имена очередей
COMPANIES_QUEUE = os.getenv('COMPANIES_QUEUE', 'companies')
CANDLES_QUEUE = os.getenv('CANDLES_QUEUE', 'candles')
TRADES_QUEUE = os.getenv('TRADES_QUEUE', 'trades')
ORDER_BOOK_QUEUE = os.getenv('ORDER_BOOK_QUEUE', 'order_book')

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание клиента ClickHouse
clickhouse_client = ClickHouseClient(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
    settings={'connect_timeout': 60, 'send_receive_timeout': 300}
)

async def insert_data(table_name, data):
    """Вставляет данные в указанную таблицу."""
    if not data:
        return

    try:
        if table_name == 'candles':
            query = """
            INSERT INTO candles (company_id, timestamp, open, high, low, close, volume)
            VALUES
            """
            data_tuples = [
                (
                    row['company_id'],
                    row['timestamp'],  # Объект datetime
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume']
                ) for row in data
            ]
        elif table_name == 'trades':
            query = """
            INSERT INTO trades (company_id, timestamp, price, volume, side)
            VALUES
            """
            data_tuples = [
                (
                    row['company_id'],
                    row['timestamp'],  # Объект datetime
                    row['price'],
                    row['volume'],
                    row['side']
                ) for row in data
            ]
        elif table_name == 'order_book':
            query = """
            INSERT INTO order_book (company_id, timestamp, bid_price, bid_volume, ask_price, ask_volume)
            VALUES
            """
            data_tuples = [
                (
                    row['company_id'],
                    row['timestamp'],  # Объект datetime
                    row['bid_price'],
                    row['bid_volume'],
                    row['ask_price'],
                    row['ask_volume']
                ) for row in data
            ]
        elif table_name == 'companies':
            query = """
            INSERT INTO companies (company_id, name, ticker, sector)
            VALUES
            """
            data_tuples = [
                (
                    row['company_id'],
                    row['name'],
                    row['ticker'],
                    row['sector']
                ) for row in data
            ]
        else:
            logger.warning(f"Unknown table: {table_name}")
            return

        clickhouse_client.execute(query, data_tuples)
        logger.info(f"Inserted {len(data)} records into {table_name}")

    except Exception as e:
        logger.error(f"Failed to insert data into {table_name}: {e}")

def validate_message(message, table_name):
    """Проверяет, содержит ли сообщение все необходимые поля."""
    if table_name == 'candles':
        required_fields = ['company_id', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
    elif table_name == 'trades':
        required_fields = ['company_id', 'timestamp', 'price', 'volume', 'side']
    elif table_name == 'order_book':
        required_fields = ['company_id', 'timestamp', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume']
    elif table_name == 'companies':
        required_fields = ['company_id', 'name', 'ticker', 'sector']
    else:
        logger.warning(f"Unknown table: {table_name}")
        return False

    for field in required_fields:
        if field not in message:
            logger.error(f"Missing field: {field}")
            return False
    return True

async def process_message(message):
    """Обработчик сообщений из RabbitMQ."""
    try:
        message_body = json.loads(message.body)
        logger.info(f"Received message from queue: {message_body}")

        # Определение типа данных
        if 'open' in message_body:
            table_name = 'candles'
        elif 'side' in message_body:
            table_name = 'trades'
        elif 'bid_price' in message_body:
            table_name = 'order_book'
        elif 'name' in message_body:  # Используем 'name' для определения компаний
            table_name = 'companies'
        else:
            logger.warning("Received message with unknown format")
            return

        if not validate_message(message_body, table_name):
            return

        # Преобразование 'timestamp' из строки в объект datetime
        if 'timestamp' in message_body:
            try:
                message_body['timestamp'] = datetime.strptime(message_body['timestamp'], '%Y-%m-%d %H:%M:%S')
            except ValueError as ve:
                logger.error(f"Timestamp format error: {ve}")
                return

        await insert_data(table_name, [message_body])
        await message.ack()
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

async def main():
    while True:
        try:
            connection = await connect_robust(
                f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}/",
                client_properties={"connection_name": "rabbitmq_to_clickhouse"}
            )

            async with connection:
                channel = await connection.channel()

                # Объявление очередей
                queues = [COMPANIES_QUEUE, CANDLES_QUEUE, TRADES_QUEUE, ORDER_BOOK_QUEUE]
                for queue_name in queues:
                    queue = await channel.declare_queue(queue_name, durable=True)
                    await queue.consume(process_message, no_ack=False)

                logger.info('Ожидание сообщений. Для выхода нажмите CTRL+C')
                await asyncio.Future()  # Run forever

        except (AMQPConnectionError, ChannelInvalidStateError) as e:
            logger.error(f"Connection error: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            break

if __name__ == "__main__":
    asyncio.run(main())
