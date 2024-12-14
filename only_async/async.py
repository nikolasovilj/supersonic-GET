import asyncio
import aiohttp
import time
import pandas as pd

df = pd.read_csv('../dataset/url-cleaned.csv')

urls = df['url'].to_list()[:10000]

c = 0
results = []
cf = 0

async def fetch_page(session, url):
    try:
        async with session.get(url, ssl=True, allow_redirects=False) as response:
            global c
            c += 1
            global cf
            print(c, cf)
            try:
                if response.status == 200:
                    print("[{}] from {}".format(response.status, url))
                    cf += 1
                    return (url, await response.text())
                #print("starting {}".format(url))
                #print("HISTORY:")
                #print("{}\n".format(response.history))
            except Exception as ex:
                print("exception within!")
                print(ex)
                time.sleep(1)
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
        results = await asyncio.gather(*tasks, return_exceptions=True)

df_responses = pd.DataFrame()

FILE_NAME = "shifting_results.csv"

with open(FILE_NAME, 'w') as f:
    f.write("url,html\n")

while urls:
    # urls to work on
    urls_tw = urls[0:1000]
    del urls[0:1000]
    
    asyncio.run(download_all_sites(urls_tw))
    print(type(results))
    print(len(results))
    results2 = list(filter(lambda x: x != None and not isinstance(x, Exception), results)) 
    print(len(results2))
    #time.sleep(3)
    tmp_df = pd.DataFrame(results2)
    print(tmp_df)
    tmp_df.to_csv(FILE_NAME, mode='a', index=False, header=False)
    #time.sleep(3)
    print(tmp_df.shape)

print("---------------------------------------------------------")
print("FINISHED!!!")
