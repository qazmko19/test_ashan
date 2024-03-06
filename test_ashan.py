import pickle
from urlextract import URLExtract

data = None

with open('messages_to_parse.dat', 'rb') as f:
    data = pickle.load(f)

extractor = URLExtract()

urls_data = extractor.find_urls(data)

print(urls_data)
