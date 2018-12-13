import requests
from requests.adapters import HTTPAdapter
import time
def simpleRule_dis(key,response):
    # response = response.json()
    result_list=[]
    for hit in response['hits']['hits']:
        freebase_label = hit['_source']['label']
        freebase_id = hit['_source']['resource']
        score=hit.get('_score')
        if key.lower() in freebase_label.lower():
            return (( freebase_label,freebase_id,score))
        result_list.append(( freebase_label,freebase_id,score))
    #hits = response['hits']['hits']
    #return sorted([(i['_source']['label'],i['_source']['resource'],i["_score"]) for i in hits],key=lambda e:e[2],reverse=True).pop(0)
    if len(result_list):
        return result_list.pop(0)
    else:
        return []
def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    #print(domain)
    #s=requests.Session()
    #s.mount('http://', HTTPAdapter(max_retries=3))
    #s.mount('https://', HTTPAdapter(max_retries=3))
    i=5
    #try:
    while(i):
        try:
            response = requests.get(url, params={'q': query[1], 'size':100})
            break
        except:
            time.sleep(0.1)
            i-=1
            response=None
            continue
    #except:
        #id_labels=query
    # id_labels = {}
    # #print(response)
    #    time.sleep(0.1)
    #    try:
    #        response = requests.get(url, params={'q': query, 'size':100})
    #    except:
    #        return query
    id_labels=None
    if response:
        #try:
        response = response.json()
        #except:
            #yield query[1]
        #print(response)
        
        id_labels=simpleRule_dis(query[1],response)
        
    return (query[0],query[1],str(id_labels))
def file2tuple(file):
    file_id,mention_list=file
    for mention in mention_list:
        yield (file_id,mention[0])
def filter(mention):
    
    if len(mention[1].split(' '))>4:
        return 0
    if mention[2] == 'PERSON' or mention[2] == 'ORG' or mention[2] =='GPE':
        if '  ' in mention[1]:
            return 0
        return 1
if __name__ == '__main__':
    import sys

    try:
        INPUT = sys.argv[1]
        OUTPUT = sys.argv[2]
        EHost = sys.argv[3]
    except Exception as e:
        # print('Usage: python starter-code.py INPUT')
        sys.exit(0)
    with open(INPUT,'r') as f:
        with open(OUTPUT,'w') as fo:
            for item in f.readlines():
                mention=eval(item)
                #print(mention)
                if filter(mention):
                    fo.writelines(str(search(EHost,mention))+'\n')
