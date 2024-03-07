# Імпорт необхідних бібліотек/модулів
import pickle
from urllib.parse import urlparse
from urlextract import URLExtract
import requests


def deserialization(file):
    with open(file, 'rb') as f:
        data_list = pickle.load(f)

    return ' '.join(data_list)


def url_extract(data):
    extractor = URLExtract()

    return extractor.find_urls(data)


def fill_status_codes(urls_data):
    status_codes = {}
    for i in range(len(urls_data)):
        url = urls_data[i]

        try:
            response = requests.head(url, timeout=5)
        except requests.exceptions.MissingSchema:
            response = requests.head('https://' + urls_data[i])
        except requests.exceptions.RequestException:
            status_codes[url] = -1
            continue

        status_code = response.status_code
        status_codes[url] = status_code

    return status_codes


def status_dictionary(status_codes):
    for url, status_code in status_codes.items():
        print(f"URL: {url}, Status code: {status_code}")


def fill_final_urls(urls_data):
    final_urls = {}
    for i in range(len(urls_data)):
        url = urls_data[i]

        try:
            response = requests.head(url, allow_redirects=True, timeout=5)
        except requests.exceptions.MissingSchema:
            response = requests.head('https://' + urls_data[i])
        except requests.exceptions.RequestException:
            continue

        final_url = response.url
        parsed_original = urlparse(url)
        parsed_final = urlparse(final_url)

        if is_short_url(parsed_original, parsed_final):
            final_urls[url] = final_url

    return final_urls


def final_urls_dictionary(final_urls):
    for url, final_url in final_urls.items():
        print(f"URL: {url}, Final URL: {final_url}")


def is_short_url(original, final):
    return original.netloc != final.netloc


if __name__ == "__main__":
    data = deserialization("messages_to_parse.dat")
    urls = url_extract(data)
    status_codes = fill_status_codes(urls)
    status_dictionary(status_codes)
    final_urls = fill_final_urls(urls)
    final_urls_dictionary(final_urls)

