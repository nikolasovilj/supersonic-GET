import asyncio
import aiohttp
from multiprocessing import Process, current_process, Queue, cpu_count
import time
import pandas as pd
import sys

df = pd.read_csv("dataset/url-cleaned.csv")
lista = df['url'].to_list()
lista = lista[:10000]

results = []
c = 0
cf = 0

async def fetch_page(session, url):
    try:
        async with session.get(url, ssl=False, allow_redirects=False) as response:
            global c
            c += 1
            global cf
            #print(c, cf)
            try:
                if response.status == 200:
                    print("[{}] from {}".format(response.status, url))
                    cf += 1
                    return (url, await response.text())
            except Exception as e:
                print("exception within!!!")
                print(e)
    except Exception as e:
        c += 1
        print("exception!")
        print(e)

timeout = aiohttp.ClientTimeout(total=60)

async def download_all_sites(sites):
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for url in sites:
            task = asyncio.ensure_future(fetch_page(session, url))
            tasks.append(task)
        global results
        results = await asyncio.gather(*tasks, return_exceptions=False)

def dummy_async(urls):
    asyncio.get_event_loop().run_until_complete(download_all_sites(urls))
    results_2 = list(filter(lambda x: x != None, results))
    return results_2

class Downloader(Process):
    def __init__(self, queue, wq):
        Process.__init__(self)
        self.queue = queue
        self.wq = wq

    def run(self):
        while True:
            if self.queue.empty():
                print(f"{current_process().name} queue is empty!")
                time.sleep(0.1)
            else:
                lista = self.queue.get()
                if type(lista) == list:
                    print(f"number of remaining URLs: {len(lista)}")
                    temp = lista[:1000]
                    del lista[:1000]
                    if lista:
                        self.queue.put(lista)
                    else:
                        print(f"{current_process().name} putting 0")
                        self.queue.put(0)
                    res_to_write = dummy_async(temp)
                    self.wq.put(res_to_write)
                else:
                    self.queue.put(lista)
                    self.wq.put(1)
                    return

class Writer(Process):
    def __init__(self, wq, cpus):
        Process.__init__(self)
        self.wq = wq
        self.cpus = cpus

    def run(self):
        counter = 0
        while True:
            if self.wq.empty():
                #print(f"{current_process().name} -> queue is empty!")
                time.sleep(0.1)
                #print("counter: ", counter)
                if counter == self.cpus:
                    print("Downloaders finished downloading.")
                    return
            else:
                results_tw = self.wq.get()
                if type(results_tw) == int:
                    counter += results_tw
                    pass
                else:
                    #print("results_tw_type", type(results_tw))
                    #print("results_tw_len: ", len(results_tw))
                    tmp_df = pd.DataFrame(results_tw)
                    #print("shape: ", tmp_df.shape)
                    print(tmp_df)
                    tmp_df.to_csv("url-html.csv", mode='a', index=False, header=False)

if __name__ == "__main__":

    wq = Queue()
    q = Queue()
    q.put(lista)
    del lista
    
    print("Creating CSV file...")
    with open("url-html.csv", "w") as f:
        f.write("url,html\n")
    
    print("Starting writer process....")
    CPUs = cpu_count()
    w = Writer(wq, CPUs)
    w.start()
    
    print("Starting downloader processes...")
    downloaders = [Downloader(q, wq) for x in range(CPUs)]
    
    for x in downloaders:
        x.start()
    
    for x in downloaders:
        x.join()

    w.join()

    sys.exit(0)
