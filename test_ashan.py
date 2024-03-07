# Импорт необходимых модулей/библиотек
import os
import pickle
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse
import requests.exceptions
from loguru import logger
from urlextract import URLExtract

# Начало работы программы
start_time = time.time()

# Настройки логирования: директория, название файла, частота создания
logger.add('logs/{time:YYYY-MM-DD_HH-mm-ss}.log', rotation='5 minutes')
LOGS_DIR = "logs/"

# Создать директорию если её нет
os.makedirs(LOGS_DIR, exist_ok=True)

# Десериализация файла
with open("messages_to_parse.dat", 'rb') as f:
    data_list = pickle.load(f)

# Приведение данных к типу данных str
data = ' '.join(data_list)

# Создание объекта класса URLExtract
extractor = URLExtract()

# Поиск всех URL адресов
urls = extractor.find_urls(data)

# Создание необходимых словарей
status_codes = {}
final_urls = {}


# Функция удаления файлов логирования старше 20 минут
def delete_old_logs():
    now = datetime.now()
    for log_file in os.listdir(LOGS_DIR):
        log_file_path = os.path.join(LOGS_DIR, log_file)
        creation_time = datetime.fromtimestamp(os.path.getctime(log_file_path))
        if now - creation_time > timedelta(minutes=20):
            os.remove(log_file_path)
            logger.info(f"Deleted old log file: {log_file_path}")


# Выполнение функции удаления старых файлов логирования
delete_old_logs()
logger.info("Started deleting old log files")
logger.info("Starting processing URLs...")


# Функция заполнения словаря с ключами - URL и значениями - Status code
def fill_status_codes(fsc_url):
    try:
        response = requests.head(fsc_url, timeout=5)
    except requests.exceptions.MissingSchema:
        response = requests.head('https://' + fsc_url, timeout=5)
        logger.warning(f"{fsc_url} is having MissingSchema exception. Request send to {'https://' + fsc_url}")
    except requests.exceptions.RequestException:
        logger.error(f"{fsc_url} is unreachable. Status code of this URL is -1")
        status_codes[fsc_url] = -1
        return

    logger.info(f"URL: {fsc_url}; Status code: {response.status_code}")
    status_codes[fsc_url] = response.status_code


# Функция заполнения словаря с ключами - оригинальный URL и значениями - финальный URL
def fill_final_urls(ffu_url):
    try:
        response = requests.head(ffu_url, allow_redirects=True, timeout=5)
    except requests.exceptions.MissingSchema:
        response = requests.head('https://' + ffu_url, allow_redirects=True, timeout=5)
        logger.warning(f"{ffu_url} is having MissingSchema exception. Request send to {'https://' + ffu_url}")
    except requests.exceptions.RequestException:
        logger.error(f"{ffu_url} is unreachable. Unable to check short URL")
        return

    parsed_original = urlparse(ffu_url)
    parsed_final = urlparse(response.url)

    if parsed_original.netloc != parsed_final.netloc:
        logger.info(f"URL: {ffu_url}; Final URL: {response.url}")
        final_urls[ffu_url] = response.url


# Использование потоков для ускорения работы функции fill_status_codes()
threads = []
for url in urls:
    thread = threading.Thread(target=fill_status_codes, args=(url,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

# Использование потоков для ускорения работы функции fill_final_urls()
threads = []
for url in urls:
    thread = threading.Thread(target=fill_final_urls, args=(url,))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()

# Конец отсчёта времени работы программы
end_time = time.time()

# for url, status_code in status_codes.items():
#     print(f"URL: {url}; Status code: {status_code}")
#
# for url, final_url in final_urls.items():
#     print(f"URL: {url}; Final URL: {final_url}")

# Разделение времени работы программы на минуты, секунды и миллисекунды
result_time = end_time - start_time

minutes = int(result_time // 60)
seconds = int(result_time % 60)
milliseconds = int((result_time - seconds) * 1000)

# Информация о времени работы программы
logger.info(f"Finished processing URLs. Time is {minutes:02d}:{seconds:02d}:{milliseconds:02d}")

# Удаление старых файлов логирования
delete_old_logs()
logger.info("Finished deleting old log files")

print(len(status_codes))
print(len(final_urls))
