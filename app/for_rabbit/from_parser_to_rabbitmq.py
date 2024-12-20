import os
from datetime import timedelta, datetime
from tqdm import tqdm
from tinkoff.invest import CandleInterval, Client
from tinkoff.invest.utils import now
import time
import logging
import pika
import json
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

TOKEN = os.getenv('TOKEN')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 5672))
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'rmuser')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', 'rmpassword')

# Имена очередей
COMPANIES_QUEUE = os.getenv('COMPANIES_QUEUE', 'companies')
CANDLES_QUEUE = os.getenv('CANDLES_QUEUE', 'candles')
TRADES_QUEUE = os.getenv('TRADES_QUEUE', 'trades')
ORDER_BOOK_QUEUE = os.getenv('ORDER_BOOK_QUEUE', 'order_book')

# Проверка наличия необходимых переменных
if not TOKEN:
    raise ValueError("Переменная окружения TOKEN не установлена.")

def get_all_companies(client) -> list:
    """Получает все компании (акции) доступные через API."""
    shares = client.instruments.shares()
    return shares.instruments

def get_candles(figi, client, interval=CandleInterval.CANDLE_INTERVAL_HOUR, start_date=None, end_date=None):
    """Получает исторические данные свечей для заданного FIGI за указанный период и сразу отправляет их в RabbitMQ."""
    if start_date is None:
        start_date = now() - timedelta(days=365 * 1)  # По умолчанию за последний год
    if end_date is None:
        end_date = now()

    current_date = start_date

    while current_date < end_date:
        next_date = min(current_date + timedelta(days=30), end_date)  # Запрос данных за 30 дней
        retries = 3
        while retries > 0:
            try:
                candles_chunk = client.get_all_candles(
                    figi=figi,
                    from_=current_date,
                    to=next_date,
                    interval=interval,
                )
                for candle in candles_chunk:
                    candle_dict = {
                        'company_id': figi,
                        'timestamp': candle.time.strftime('%Y-%m-%d %H:%M:%S'),
                        'open': candle.open.units + candle.open.nano / 1e9,
                        'high': candle.high.units + candle.high.nano / 1e9,
                        'low': candle.low.units + candle.low.nano / 1e9,
                        'close': candle.close.units + candle.close.nano / 1e9,
                        'volume': candle.volume
                    }
                    send_to_rabbitmq(candle_dict, CANDLES_QUEUE)
                current_date = next_date  # Переходим к следующему периоду только после успешного запроса
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                retries -= 1
                if retries == 0:
                    reset_time = getattr(e, 'metadata', {}).get('ratelimit_reset', 0)
                    logger.warning(f"Rate limit exceeded. Waiting for {reset_time} seconds.")
                    time.sleep(reset_time)
                else:
                    time.sleep(1)  # Задержка перед повторной попыткой

def get_last_trades(figi, client, start_date=None, end_date=None):
    """Получает последние сделки для заданного FIGI за указанный период и сразу отправляет их в RabbitMQ."""
    if start_date is None:
        start_date = now() - timedelta(days=365 * 1)  # По умолчанию за последний год
    if end_date is None:
        end_date = now()

    current_date = start_date

    while current_date < end_date:
        next_date = min(current_date + timedelta(days=30), end_date)  # Запрос данных за 30 дней
        retries = 3
        while retries > 0:
            try:
                trades_chunk = client.market_data.get_last_trades(
                    figi=figi,
                    from_=current_date,
                    to=next_date
                )
                for trade in trades_chunk.trades:
                    trade_dict = {
                        'company_id': figi,
                        'timestamp': trade.time.strftime('%Y-%m-%d %H:%M:%S'),
                        'price': trade.price.units + trade.price.nano / 1e9,
                        'volume': trade.quantity,
                        'side': 'buy' if trade.direction == 1 else 'sell'
                    }
                    send_to_rabbitmq(trade_dict, TRADES_QUEUE)
                current_date = next_date  # Переходим к следующему периоду только после успешного запроса
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                retries -= 1
                if retries == 0:
                    reset_time = getattr(e, 'metadata', {}).get('ratelimit_reset', 0)
                    logger.warning(f"Rate limit exceeded. Waiting for {reset_time} seconds.")
                    time.sleep(reset_time)
                else:
                    time.sleep(1)  # Задержка перед повторной попыткой

def get_close_prices(figi, client, start_date=None, end_date=None):
    """Получает исторические данные цен закрытия для заданного FIGI за указанный период."""
    if start_date is None:
        start_date = now() - timedelta(days=365 * 1)  # По умолчанию за последний год
    if end_date is None:
        end_date = now()

    close_prices = []
    current_date = start_date

    while current_date < end_date:
        next_date = min(current_date + timedelta(days=30), end_date)  # Запрос данных за 30 дней
        retries = 3
        while retries > 0:
            try:
                candles_chunk = client.market_data.get_candles(
                    figi=figi,
                    from_=current_date,
                    to=next_date,
                    interval=CandleInterval.CANDLE_INTERVAL_DAY
                )
                close_prices.extend([candle.close.units + candle.close.nano / 1e9 for candle in candles_chunk.candles])
                current_date = next_date  # Переходим к следующему периоду только после успешного запроса
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                retries -= 1
                if retries == 0:
                    reset_time = getattr(e, 'metadata', {}).get('ratelimit_reset', 0)
                    logger.warning(f"Rate limit exceeded. Waiting for {reset_time} seconds.")
                    time.sleep(reset_time)
                else:
                    time.sleep(1)  # Задержка перед повторной попыткой

def company_to_dict(figi, name, ticker, sector):
    """Преобразует информацию о компании в словарь."""
    return {
        'company_id': figi,
        'name': name,
        'ticker': ticker,
        'sector': sector,
    }

def send_to_rabbitmq(data_dict, queue_name):
    """Отправляет данные в RabbitMQ."""
    message = json.dumps(data_dict)
    try:
        channel.basic_publish(exchange='', routing_key=queue_name, body=message)
        logger.info(f"Message sent to {queue_name}: {message}")
    except Exception as e:
        logger.error(f"Failed to send message to {queue_name}: {e}")

def get_order_book(figi, client, start_date=None, end_date=None, depth=20):
    """Получает стакан заявок для заданного FIGI за каждые 15 минут указанного периода и сразу отправляет их в RabbitMQ."""
    if start_date is None:
        start_date = now() - timedelta(days=365 * 1)  # По умолчанию за последний год
    if end_date is None:
        end_date = now()

    current_date = start_date

    while current_date < end_date:
        next_date = min(current_date + timedelta(minutes=15), end_date)  # Запрос данных за 15 минут
        retries = 3
        while retries > 0:
            try:
                order_book = client.market_data.get_order_book(
                    figi=figi,
                    depth=depth
                )
                bid_price = order_book.bids[0].price.units + order_book.bids[0].price.nano / 1e9
                bid_volume = order_book.bids[0].quantity
                ask_price = order_book.asks[0].price.units + order_book.asks[0].price.nano / 1e9
                ask_volume = order_book.asks[0].quantity
                order_book_dict = {
                    'company_id': figi,
                    'timestamp': current_date.strftime('%Y-%m-%d %H:%M:%S'),
                    'bid_price': bid_price,
                    'bid_volume': bid_volume,
                    'ask_price': ask_price,
                    'ask_volume': ask_volume
                }
                send_to_rabbitmq(order_book_dict, ORDER_BOOK_QUEUE)
                current_date = next_date  # Переходим к следующему периоду только после успешного запроса
                break
            except Exception as e:
                logger.error(f"Ошибка при получении стакана заявок для {figi} на {current_date}: {e}")
                retries -= 1
                if retries == 0:
                    reset_time = getattr(e, 'metadata', {}).get('ratelimit_reset', 0)
                    logger.warning(f"Превышен лимит запросов. Ожидание {reset_time} секунд.")
                    time.sleep(reset_time)
                else:
                    time.sleep(1)  # Задержка перед повторной попыткой

def main():
    with Client(TOKEN) as client:
        companies = get_all_companies(client)
        logger.info(f'COMPANIES[0]\n{companies[0]}')
        logger.info(f'LEN(COMPANIES)\n{len(companies)}')

        for company in tqdm(companies):
            figi = company.figi
            name = company.name
            ticker = company.ticker
            sector = company.sector
            company_dict = company_to_dict(figi=figi, name=name, ticker=ticker, sector=sector)
            send_to_rabbitmq(company_dict, COMPANIES_QUEUE)

            # 1. Волатильность инструмента от времени
            get_candles(figi, client)

            # 2. Влияние крупных сделок на цену
            get_last_trades(figi, client)

            # 3. Предсказуемость цены закрытия
            close_prices = get_close_prices(figi, client)
            # Анализ предсказуемости цены закрытия
            logger.info(f'\nCLOSE_PRICES\n{close_prices}')

            # 4. Дисбаланс между заявками на покупку и продажу
            # get_order_book(figi, client)

            # Здесь можно добавить логику для сохранения или анализа данных
            logger.info(f"Обработана компания {name} ({ticker})")

    connection.close()

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Настройка подключения к RabbitMQ
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    connection_params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    # Проверка существования очередей и их использование
    queues = [COMPANIES_QUEUE, CANDLES_QUEUE, TRADES_QUEUE, ORDER_BOOK_QUEUE]
    for queue_name in queues:
        try:
            channel.queue_declare(queue=queue_name, durable=True, passive=True)
            logger.info(f"Using existing queue: {queue_name}")
        except pika.exceptions.ChannelError:
            channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"Created new queue: {queue_name}")

    main()
