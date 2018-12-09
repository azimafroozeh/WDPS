from pyspark import SparkContext
import warc
from bs4 import BeautifulSoup
from selectolax.parser import HTMLParser
import stanfordCoreNLP
KEYNAME = "WARC-TREC-ID"
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

    return url, text
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
    key,text=record
    sNLP = stanfordCoreNLP.StanfordNLP(SHost,int(SPort))
    result = sNLP.ExtractEntityFromText(text, stanfordCoreNLP.simpleRule)
    yield (key,result)
    # if key and ('Google' in payload):
    #     yield key + '\t' + 'Google' + '\t' + '/m/045c7b'
SHost='http://localhost'
SPort=9000
if __name__ == '__main__':
    import sys

    try:
        _, INPUT,OUTPUT,SHost,SPort = sys.argv
        #print(SHost)
    except Exception as e:
        #print('Usage: python starter-code.py INPUT')
        sys.exit(0)
    sc = SparkContext()
    rdd = sc.newAPIHadoopFile(INPUT,
                              "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                              "org.apache.hadoop.io.LongWritable",
                              "org.apache.hadoop.io.Text",
                              conf={"textinputformat.record.delimiter": "WARC/1.0"})
    #f = warc.WARCFile(INPUT)
    html_rdd=rdd.flatMap(get_text)
    text_rdd=html_rdd.flatMap(rdd_html2text)
    #sNLP=stanfordCoreNLP.StanfordNLP()
    result_rdd=text_rdd.flatMap(get_NLPsupport)
    result_rdd.saveAsTextFile(OUTPUT)
