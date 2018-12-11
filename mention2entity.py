import requests
from requests.adapters import HTTPAdapter
import time
def simpleRule_dis(response):
    # response = response.json()
    hits = response['hits']['hits']
    return sorted([(i['_source']['label'],i['_source']['resource'],i["_score"]) for i in hits],key=lambda e:e[2],reverse=True).pop(0)
def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    response = requests.get(url, params={'q': query[1], 'size':100})
    if response:
        response = response.json()
        #print(response)
        id_labels=simpleRule_dis(response)
    return (query[0],id_labels[0],id_labels[1],id_labels[2])
def file2tuple(file):
    file_id,mention_list=file
    for mention in mention_list:
        yield (file_id,mention[0])
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
                fo.writelines(str(search(EHost,mention)))
