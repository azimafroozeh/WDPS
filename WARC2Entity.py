from glob import glob
import sys
import collections
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkContext
from selectolax.parser import HTMLParser
import requests
import time
from nltk.corpus import stopwords
KEYNAME = "WARC-TREC-ID"

def get_text_selectolax(html):
    tree = HTMLParser(html)
    if tree.body is None:
        return None
    for tag in tree.css('script'):
        tag.decompose()
    for tag in tree.css('style'):
        tag.decompose()
    text = tree.body.text(separator='\n')
    return text
def rdd_html2text(record):
    key,html=record
    yield (key,get_text_selectolax(html))

def read_doc(record, parser=get_text_selectolax):
    url = record.url
    text = None

    if url:
        payload = record.payload.read()
        header, html = payload.split(b'\r\n\r\n', maxsplit=1)
        html = html.strip()

        if len(html) > 0:
            text = parser(html)

    return url,text
def get_text(record):
    # finds google
    KEYNAME = "WARC-TREC-ID"
    _, payload = record
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    if key:
        yield (key,'\r\n\r\n'.join(payload.split('\r\n\r\n')[2:]))
def tokenizer(record):
    import nltk
    from nltk.tag import StanfordNERTagger
    from nltk.corpus import stopwords
    from nltk.tokenize import word_tokenize
    import sys
    import unicodedata

    nltk.data.path.append(os.environ.get('PWD'))
    key,text = record
    stop_words = set(stopwords.words('english'))

    tbl = dict.fromkeys(i for i in range(sys.maxunicode) if unicodedata.category(chr(i)).startswith('P')) #Remove pontuactions from text
    text_no_pontuation = text.translate(tbl)

    tokens = nltk.word_tokenize(text_no_pontuation)
    tokens = [w for w in tokens if not w in stop_words]

    for token in tokens:
        token.encode('utf-8')
    #tokens=[token.encode('utf-8') for token in tokens if token not in stopwords.words('english')]
    tagged = nltk.pos_tag(tokens)
    #print(tagged)
    #print(tokens)
    jar = './STANFORD/stanford-ner.jar'
    model = './STANFORD/english.all.3class.distsim.crf.ser.gz'
    st = StanfordNERTagger(model, jar, encoding='utf8')
    #print("ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
    #print(st)
    NERtags = st.tag(tokens)
    #print(NERtags)
    #for word in NERtags:
    yield(key,NERtags)
def simpleRule_dis(response):
    # response = response.json()
    result_list=[]
    for hit in response['hits']['hits']:
        freebase_label = hit['_source']['label']
        freebase_id = hit['_source']['resource']
        score=hit.get('_score')
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
    s=requests.Session()
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
        try:
            id_labels=simpleRule_dis(response)
            pass
        except:
            yield (query[0],response)
    yield (query[0],query[1],str(id_labels))
    #yield query[1]
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
        # EPort=sys.argv[4]
        # _, INPUT,OUTPUT,EHost,EPort = sys.argv
        # print(EHost,EPort)
    except Exception as e:
        # print('Usage: python starter-code.py INPUT')
        sys.exit(0)
    conf = SparkConf()
    conf.setAppName("HY")

    sc = SparkContext(conf=conf)
    # sc = SparkContext()
    rdd = sc.newAPIHadoopFile(INPUT,
                              "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                              "org.apache.hadoop.io.LongWritable",
                              "org.apache.hadoop.io.Text",
                              conf={"textinputformat.record.delimiter": "WARC/1.0"})
    html_rdd = rdd.flatMap(get_text)
    text_rdd=html_rdd.flatMap(rdd_html2text)
    text_rdd=text_rdd.collect()
    token_rdd=sc.parallelize(text_rdd, 100).flatMap(tokenizer)
    #token_rdd = text_rdd.flatMap(tokenizer)
    #tuple_rdd = rdd.flatMap(lambda x: [eval(x)])
    ftuple = token_rdd.flatMap(lambda x: file2tuple(x))
    result = ftuple.flatMap(lambda x: search(EHost, x))
    # print(result.take(10))
    result.saveAsTextFile(OUTPUT)
