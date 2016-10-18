import urllib.request
from bs4 import BeautifulSoup as bs, Comment
import re, json
from IPython.display import clear_output
import threading
import sys
import queue
import datetime
import time
import getopt
from urllib.parse import urljoin
from pymongo import MongoClient
from multiprocessing import Process, Queue, Lock
import multiprocessing
import requests
import eventlet
eventlet.monkey_patch(socket=True)
# CONFIG
json_file_location = 'single.json'
num_of_threads = 20
num_of_processes = 4
mongo_server = None
mongo_port = None

# Globals
status = (0, 0, None, None, None) # used to print status report on main thread
poison_pill = 'EOF' # Used to poison threads in different processes via shared queue.
EOF = 'EOF'
f = open(json_file_location, encoding='utf8')
lock = Lock()
input_lock = Lock()
passed = 0
failed = 0

client = MongoClient()
db = client['tweet-link-data']
links = db['links']
links.ensure_index("url", unique=True)

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
    if element.parent.name in ['style', 'script', '[document]', 'head', 'title', 'meta']  :
        return False
    elif re.match('<!--.*-->', str(element)) or re.match('<![endif].*', str(element)):
        return False
    return True

def proc_worker(q, r, num_of_threads):
    threads = []
    
    for i in range(num_of_threads):
        t = threading.Thread(target=thread_worker, args=(q,r,))
        t.daemon = True
        t.start()
        threads.append(t)
        sys.stdout.flush()
        
    for thread in threads:
        thread.join()

def main(argv):   
    handle_args(argv)
    open('failed.txt', 'w+').close()
    open('output.txt', 'w+').close()
    # Queues for cross Process/Thread communication
    # q - tweets are read in from json file to this queue, which are then retrieved by threads in other processes.
    q = Queue() # tweet queue
    # r - (return) extracted site text data and relevant info extracted by all active threads/processes are pushed to this
    #              queue and popped by a thread in the main processes for post processing / stat display.
    r = Queue() # return queue
    
    # Main process - thread that reads tweet data into a process-safe and thread-safe queue for distributed computing.
    t = threading.Thread(target=fill_queue_async, args=(q,))
    t.daemon = True
    t.start()
  
    # Main process - thread for processing return values and status output
    rt = threading.Thread(target=return_worker, args=(r,))
    rt.daemon = True
    rt.start()

    processes = []
    for i in range(num_of_processes):
        p = Process(target=proc_worker, args=(q,r,num_of_threads))
        p.start()
        processes.append(p)
        
    t.join()
    
    # do stat printing stuff
    try:
        while True:
            finished = True
            for p in processes:
                if p.is_alive():
                    #print("Process {} is alive.".format(p.name))
                    finished = False
                    
            clear_output(wait=True)

            print("Passed: {}, Failed: {}, Time elapsed: {}, items/second:  {} \n{}".format(status[0], status[1], status[2], status[3], status[4]))
            print("")
            sys.stdout.flush()
            time.sleep(2)
            if finished:
                return
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()

    
    f.close()
    print("Done.")

def handle_args(argv):
    try: 
        opts, args = getopt.getopt(argv, "p:t:f:", ["processes=", "threads=", "file="])
    except getopt.GetoptError:
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-p', '--processes'):
            global num_of_processes
            num_of_processes = int(arg)
        elif opt in ('-t', '--threads'):
            global num_of_threads
            print (arg)
            time.sleep(1)
            num_of_threads = int(arg)
        elif opt in ('-f', '--file'):
            global json_file_location
            json_file_location = arg

def fill_queue_async(q):
    with open(json_file_location, encoding='utf8') as f:
        while True:
            if q.qsize() < 10000:
                line = f.readline()
                if not line:

                    return
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

        error = do_queue_work(item)
        if error == 0:
                    # Write/insert into wherever
            with open('output.txt', 'a+') as f:
                f.write(json.dumps(item) + "\n")
            try:
                links.insert_one(item)
            except:
                pass
            passed += 1

        else:
            with open('failed.txt', 'a+') as f:
                f.write(item['url']+"\n")
            failed += 1
        time_delta = datetime.datetime.now() - start_time
        seconds_elapsed = time_delta.total_seconds() if time_delta.total_seconds() > 0 else 1
        average_item_per_second = int((passed + failed)/seconds_elapsed)
        #clear_output()
        status = (passed, failed, time_delta, average_item_per_second, item['url'] if 'url' in item else 'Failed: {}'.format(item['tiny_url']))
        sys.stdout.flush()
        
def do_queue_work(item):
    if not 'texts' in item and not 'images' in item:
        return 1
    else:
        return 0

def thread_worker(q, r):
    global passed, failed, total
    while(True):
        if q.empty():
            #print('CLOSING thread {} in process {}.'.format(threading.currentThread(), multiprocessing.current_process()))
            return
        try:
            tweet = q.get(timeout = 1)
            
            if not tweet:
                return
            for sub_urls in tweet['entities']['urls']:
                doc = {}
                doc['id'] = tweet['id']
                doc['tiny_url'] = sub_urls['url']
                doc['url'] = sub_urls['expanded_url']
                try:
                    with eventlet.Timeout(10):
                        response = requests.get(doc['url'], timeout=10)
                        if response.status_code != requests.codes.ok:
                            raise ValueError('Response not Ok. {} Error occurred.'.format(response.status_code))
                        html = response.text # Read source HTML from response
                        soup = bs(html, 'html.parser') # BeautifulSoup is used to extract the text from the html.
                        comments = soup.findAll(text=lambda text:isinstance(text,Comment))
                        [comment.extract() for comment in comments]
                        text = soup.findAll(text=True)

                        text_items = filter(visible, text) # Attempt to filter out text that isn't the 'primary content' of the page. 
                                                           # This is currently done by using a cookie-cutter blacklist for unneeded tags.
                        
                        # An attempt to remove spurious text (such as nav menus) and empty newlines. After cleaning whitespace,
                        # we only accept strings of text that contain more than 10 space-separated tokens (hopefully words).
                        # TODO: make this more efficient. All over the place right now.
                        visible_text = [item.strip() for item in text_items if item != '\n' and len(item.strip().split(' ')) > 10]

                        doc['texts'] = visible_text
                        doc['images'] = list(set([tag['src'] for tag in soup.findAll('img') if len(tag['src']) < 100]))
                        

                        # Update image urls to full urls
                        doc['images'] = [urljoin(url, img) for img in doc['images']]
                except:
                    #Could not access address
                    #print(tweet['entities']['urls'][0]['url'])
                    pass

                finally:
                    try:
                        r.put(doc, timeout=2)
                    except:
                        return
        except:
            return

if __name__ == '__main__':
    main(sys.argv[1:])