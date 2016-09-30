
# coding: utf-8

# In[1]:

import urllib.request
from bs4 import BeautifulSoup as bs
import re, json
from IPython.display import clear_output
import threading
import sys

JSON_FILE_LOCATION = 'tweet_data.json'

def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]

def getURLs(json_file):
    urls = []
    with open(json_file, encoding="utf8") as f:
        data = json.load(f)
        for tweet in data:
            if 'urls' in tweet['entities']:
                for url in tweet['entities']['urls']:
                    urls.append(url['url'])
    return urls

    
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)) or re.match('<![endif].*', str(element)):
        return False
    return True

    
def main():
    urls = getURLs(JSON_FILE_LOCATION)
    urls_list = chunkify(urls, 5)
    
    for urls in urls_list:
        t = threading.Thread(target=myThreadFunction, args=(urls))
        t.start
        
    print("Wtf")
    
        
        
def myThreadFunction(urls):
    passed = 0
    failed = 0
    total = len(urls)
    
    for url in urls:
        try:
            with urllib.request.urlopen(url) as response:
                passed = passed + 1
                html = response.read()
                soup = bs(html, 'html.parser')
                text = soup.findAll(text=True)

                visible_text = filter(visible, text)
                for item in visible_text:
                    item = item.replace('\n', ' ')
                    #print (item)
        except:
            failed = failed + 1
            visible_text = 'NULL'
            pass

        finally:
            completed = passed+failed
            print('Passed: {}, Failed: {}, Total: {}\n{}'.format(passed, failed, total, ''.join(visible_text)))
            sys.stdout.flush()

if __name__ == '__main__':
    main()

