import urllib.request
from bs4 import BeautifulSoup as bs
import re, json
from IPython.display import clear_output
import threading
import sys
import queue
import datetime
import time
from multiprocessing import Process, Queue, Lock

# CONFIG
JSON_FILE_LOCATION = 'single.json'
NUM_OF_THREADS = 20
NUM_OF_PROCESSES = 4

# EVIL GLOBALS
status = (0, 0, None, None, None)
poison_pill = 'EOF'
EOF = 'EOF'
f = open(JSON_FILE_LOCATION, encoding='utf8')
lock = Lock()
input_lock = Lock()
passed = 0
failed = 0

def chunkify(lst,n):
    return [lst[i::n] for i in range(n)]

def get_tweet():
    global f
    with input_lock:
        try:
            while(True):
                line = f.readline()
                if line == '':
                    return EOF
                tweet = json.loads(line)
                if len(tweet['entities']['urls']) > 0:
                    #print(tweet['entities']['urls'][0]['url'])
                    return tweet
        except:
            return EOF
    
def visible(element):
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
        return False
    elif re.match('<!--.*-->', str(element)) or re.match('<![endif].*', str(element)):
        return False
    return True

def proc_worker(q, r):
    print('proc started')
    threads = []
    
    for i in range(NUM_OF_THREADS):
        t = threading.Thread(target=thread_worker, args=(q,r,))
        t.daemon = True
        t.start()
        threads.append(t)
        
    for thread in threads:
        thread.join()
    
def main():   
    # Queue for cross Process/Thread communication
    q = Queue() # tweet queue
    r = Queue() # return queue
    
    t = threading.Thread(target=fill_queue_async, args=(q,))
    t.daemon = True
    t.start()
  
    # Thread for processing return values and status output
    rt = threading.Thread(target=return_worker, args=(r,))
    rt.daemon = True
    rt.start()

    processes = []
    for i in range(NUM_OF_PROCESSES):
        p = Process(target=proc_worker, args=(q,r,))
        p.start()
        processes.append(p)
        
    t.join()
    
    # do stat printing stuff
    while True:
        finished = True
        for p in processes:
            if p.is_alive():
                finished = False
                
        clear_output(wait=True)
        print("Passed: {}, Failed: {}, Time elapsed: {}, items/second:  {} \n{}".format(status[0], status[1], status[2], status[3], status[4]))
        sys.stdout.flush()
        time.sleep(2)
        if finished:
            break
    
    print("Done.")
    
def fill_queue_async(q):
    with open(JSON_FILE_LOCATION, encoding='utf8') as f:
        while True:
            if q.qsize() < 10000:
                line = f.readline()
                if not line:
                    q.put(poison_pill)
                    break
                tweet = json.loads(line)
                if len(tweet['entities']['urls']) > 0:
                    #print(tweet['entities']['urls'][0]['url'])
                    q.put(tweet)
                
            else:
                print('Queue full.')

def return_worker(r):
    global status
    start_time = datetime.datetime.now()
    passed = 0
    failed = 0
    while True:
        item = r.get()

        # Write/insert into wherever
        with open('test.txt', 'w+') as f:
            f.write(json.dumps(item))


        error = do_queue_work(item)
        if error == 0:
            passed += 1
        else:
            failed += 1
        time_delta = datetime.datetime.now() - start_time
        seconds_elapsed = time_delta.total_seconds() if time_delta.total_seconds() > 0 else 1
        average_item_per_second = int((passed + failed)/seconds_elapsed)
        #clear_output()
        status = (passed, failed, time_delta, average_item_per_second, item['url'] if 'url' in item else 'Failed')
        sys.stdout.flush()
        
def do_queue_work(item):
    if item == None:
        return 1
    else:
        return 0

def thread_worker(q, r):
    global passed, failed, total
    while(True):
        tweet = q.get()
        doc = {}
        if tweet == poison_pill:
            q.put(poison_pill)
            #print('thread poisoned. thread dead.')
            break
        else:
            try:
                doc['id'] = tweet['id']
                for sub_urls in tweet['entities']['urls']:
                    with urllib.request.urlopen(sub_urls['url']) as response:
                        html = response.read()
                        url = response.geturl()
                        soup = bs(html, 'html.parser')
                        text = soup.findAll(text=True)
                        text_items = filter(visible, text)
                        visible_text = [item.strip() for item in text_items if item != '\n' and len(item.strip().split(' ')) > 10]
                        
                        doc['texts'] = visible_text
                        doc['images'] = list(set([tag['src'] for tag in soup.findAll('img')]))
                        doc['url'] = url
                        doc['tiny_url'] = sub_urls['url']
            except:
                #Could not access address
                #print(tweet['entities']['urls'][0]['url'])
                pass

            finally:
                r.put(doc)

if __name__ == '__main__':
    main()