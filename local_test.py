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
conf = SparkConf()
conf.setAppName("AzimTest1")
from nltk.tag import StanfordNERTagger
sc = SparkContext(conf=conf)

KEYNAME = "WARC-TREC-ID"
INFILE = sys.argv[1]
OUTFILE = sys.argv[2]
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
    jar = 'stanford-ner.jar'
    model = 'english.all.3class.distsim.crf.ser.gz'
    st = StanfordNERTagger(model, jar, encoding='utf8')
    key,text = record
    tokens = nltk.word_tokenize(text)
    for token in tokens:
        token.encode('utf-8')
    tagged = nltk.pos_tag(tokens)
    #print(tagged)
    #print(tokens)
    print("ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
    #print(st)
    NERtags = st.tag(tokens)
    print(NERtags)
    yield(key,NERtags)

rdd = sc.newAPIHadoopFile(INFILE,
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

#rdd0 = rdd.flatMap(find_google)
#rdd0.saveAsTextFile(OUTFILE)
#print("begin")
#sNLP = StanfordNLP(SHost,int(SPort))
#print(sNLP)
rdd1 = rdd.flatMap(get_text)
#print(rdd1.take(10))
#print("====================================================================================================================================")
rdd2 = rdd1.flatMap(rdd_html2text)
#print(rdd2.take(10))
#print("====================================================================================================================================")
#rdd3 = rdd2.flatMap(get_NLPsupport)
rdd3 = rdd2.flatMap(tokenizer)
rdd3.saveAsTextFile(OUTFILE)
