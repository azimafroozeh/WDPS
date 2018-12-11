from glob import glob
import sys
import collections
import os
import sys
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark import SparkContext
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
from stanfordcorenlp import StanfordCoreNLP
import requests
import time
import re
conf = SparkConf()
conf.setAppName("AzimTest1")

sc = SparkContext(conf=conf)

KEYNAME = "WARC-TREC-ID"
INFILE = sys.argv[1]
OUTFILE = sys.argv[2]
EHost = sys.argv[3]
SHost='http://node062'
SPort=9000
def get_text_bs(html):
    tree = BeautifulSoup(html, 'lxml')

    body = tree.body
    if body is None:
        return None

    for tag in body.select('script'):
        tag.decompose()
    for tag in body.select('style'):
        tag.decompose()

    text = body.get_text(separator='\n')
    return text


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
def get_NLPsupport(record):
    #print(record)
    key,text=record
    #sNLP = StanfordNLP(SHost,int(SPort))
    #print(sNLP)
    result = sNLP.ExtractEntityFromText(text, simpleRule)
    #print(key,result)
    yield (key,result)
    # if key and ('Google' in payload):
    #     yield key + '\t' + 'Google' + '\t' + '/m/045c7b'
def tokenizer(record):
    import nltk
    from nltk.tag import StanfordNERTagger
    nltk.data.path.append(os.environ.get('PWD'))
    key,text = record
    #tokens = nltk.word_tokenize(text)
    #for token in tokens:
    #    token.encode('utf-8')
    #tagged = nltk.pos_tag(tokens)
    #print(tagged)
    #print(tokens)
    #jar = './STANFORD/stanford-ner.jar'
    #model = './STANFORD/english.all.3class.distsim.crf.ser.gz'
    #st = StanfordNERTagger(model, jar, encoding='utf8')
    #print("ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
    #print(st)
    #NERtags = st.tag(tokens)
    #print(NERtags)
    #yield(key,NERtags)
    a = sys.path
    b = '/cm/shared/package/python/3.5.2'
    c = '/cm/shared/package/python/3.5.2/lib/python3.5/site-packages'
    if b in a:
        sys.path.remove('/cm/shared/package/python/3.5.2')
    if c in a:
        sys.path.remove('/cm/shared/package/python/3.5.2/lib/python3.5/site-packages')
    import spacy
    from spacy import displacy
    from collections import Counter
    import en_core_web_sm
    nlp = en_core_web_sm.load()
    print(len(nlp.vocab))
    doc = nlp(text)
    yield(key,doc.ents)
rddinput = sc.newAPIHadoopFile(INFILE,
    "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
    "org.apache.hadoop.io.LongWritable",
    "org.apache.hadoop.io.Text",
    conf={"textinputformat.record.delimiter": "WARC/1.0"})
def find_google(record):
    # finds google
    _, payload = record
    key = None
    for line in payload.splitlines():
        if line.startswith(KEYNAME):
            key = line.split(': ')[1]
            break
    if key and ('http' in payload):
        yield key + '\t' + 'http' + '\t' + '/m/045c7b'
import logging
import json
class StanfordNLP:
    def __init__(self, host='http://localhost', port=9000):
        self.nlp = StanfordCoreNLP(host, port=port,
                                   timeout=300000)  # , quiet=False, logging_level=logging.DEBUG)
        self.props = {
            'annotators': 'tokenize,ssplit,pos,lemma,ner,parse,depparse,dcoref,relation',
            'pipelineLanguage': 'en',
            'outputFormat': 'json'
        }

    def word_tokenize(self, sentence):
        return self.nlp.word_tokenize(sentence)

    def pos(self, sentence):
        return self.nlp.pos_tag(sentence)

    def ner(self, sentence):
        return self.nlp.ner(sentence)

    def parse(self, sentence):
        return self.nlp.parse(sentence)

    def dependency_parse(self, sentence):
        return self.nlp.dependency_parse(sentence)

    def annotate(self, sentence):
        return json.loads(self.nlp.annotate(sentence, properties=self.props))

    @staticmethod
    def tokens_to_dict(_tokens):
        tokens = defaultdict(dict)
        for token in _tokens:
            tokens[int(token['index'])] = {
                'word': token['word'],
                'lemma': token['lemma'],
                'pos': token['pos'],
                'ner': token['ner']
            }
        return tokens
    def ExtractEntityFromText(self,text,rule):
        text=text.replace('\t','\n')
        paragraphs=text.split('\n')
        for i  in paragraphs:
            if i=='':
                paragraphs.remove(i)
        result=[]
        #print(paragraphs)
        for para in paragraphs:
            try:
                ann=self.annotate(para)
                #print(ann)
                pos=self.pos(para)
                #print(pos)
                ner=self.ner(para)
                #print(ner)
            except:
                pass
                #print(para)
            #print(ann,pos,ner)
            result.extend(rule(ann,pos,ner))
        return result

def simpleRule(ann,pos,ner):
    tmp=[]
    for p,n in zip(pos,ner):
        if p[1]=='NNP' or p[1]=='NN':
            tmp.append(n)
    special=None
    special_flag=0
    special_type=None
    result=[]
    #print(tmp)
    for i in tmp:
        if special_flag==1:
            if i[1]=='O':
                result.append((special,special_type))
                special=None
                special_flag=0
                result.append(i)
            else:
                if special_type==i[1]:
                    special+=' '+i[0]
                else:
                    result.append((special,special_type))
                    special=i[0]
                    special_type=i[1]
        else:
            if i[1]=='O':
                result.append(i)
            else:
                special=i[0]
                special_flag=1
                special_type=i[1]
    if special_flag==1:
        result.append((special,special_type))
    return result

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
def search(query):
    url = 'http://%s/freebase/label/_search' % EHost
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

def html_to_string(record):
    a = sys.path
    b = '/cm/shared/package/python/3.5.2'
    c = '/cm/shared/package/python/3.5.2/lib/python3.5/site-packages'
    if b in a:
        sys.path.remove('/cm/shared/package/python/3.5.2')
    if c in a:
        sys.path.remove('/cm/shared/package/python/3.5.2/lib/python3.5/site-packages')
    import spacy
    from spacy import displacy
    from collections import Counter
    import en_core_web_sm
    nlp = en_core_web_sm.load()
    #print(len(nlp.vocab))
    key, text = record
    html = text
    #print(html)
    soup = BeautifulSoup(html, 'html5lib')
    for script in soup(['head', 'title', 'meta', '[document]',"script", "style", 'aside']):
        script.extract()
    #print(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    #print("===================================")
    article = nlp(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    #print(article) 
    #print("articcccccccccccccccccccccccccle")
    for x in article.sents:
        # print(type(x))
        #print(x)
        [(x.orth_,x.pos_, x.lemma_) for x in [y 
                                      for y
                                      in nlp(str(x)) 
                                      if not y.is_stop and y.pos_ != 'PUNCT']]
        z = dict([(str(x), x.label_) for x in nlp(str(x)).ents])
        #print(z)
        for k,v in z.items():
            #print(k)
            #print(v)
            yield(key, k,v)

#rdd0 = rdd.flatMap(find_google)
#rdd0.saveAsTextFile(OUTFILE)
#print("begin")
#sNLP = StanfordNLP(SHost,int(SPort))
#print(sNLP)
rdd1 = rddinput.flatMap(get_text)
#print(rdd1.take(10))
#print("====================================================================================================================================")
temp = rdd1.take(100)
rdd2 = sc.parallelize(temp, 100).flatMap(html_to_string)
#print(rdd2.take(10))
#print("====================================================================================================================================")
#rdd3 = rdd2.flatMap(get_NLPsupport)
#rdd3 = rdd2.flatMap(tokenizer)
#rdd4 = rdd2.flatMap(file2tuple)
#result = rdd2.flatMap(search).saveAsTextFile(OUTFILE)
rdd2.saveAsTextFile(OUTFILE)
