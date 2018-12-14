from bs4 import BeautifulSoup
import en_core_web_lg
nlp = en_core_web_lg.load()
import re
import warc
def split_records(stream):
    payload = ''
    for line in stream:
        if line.strip() == "WARC/1.0":
            yield payload
            payload = ''
        else:
            payload += line
def html_to_string(record):
    url = record.url
    text = None

    if url:
        payload = record.payload.read()
    #print(len(nlp.vocab))

    header, html = payload.split(b'\r\n\r\n', maxsplit=1)
    html = html.strip()

    if len(html) == 0:
        return 0
    #print(html)
    soup = BeautifulSoup(html, 'html5lib')
    for script in soup(['head', 'title', 'meta', '[document]',"script", "style", 'aside']):
        script.extract()
    #print(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    #print("===================================")
    return soup.get_text()
    article = nlp(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    #print(article)
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
            return ( k,v)
if __name__ == '__main__':
    import sys

    try:
        _, INPUT,OUTPUT = sys.argv
        print(OUTPUT)
    except Exception as e:
        print('Usage: python starter-code.py INPUT')
        sys.exit(0)
    f = warc.WARCFile(INPUT)
    f_out = open(OUTPUT,'a+')
    for record in f:
    #sNLP = stanfordCoreNLP.StanfordNLP()
        # print(record.header)
        # id_=record.header._d["warc-trec-id"]
        # print(id_)
        if record.header._d["warc-type"] == 'response':
            print(record.header._d["warc-trec-id"])
            if '27' not in record.header._d["warc-trec-id"]:
                continue

            tuple_k=html_to_string(record)
            print(tuple_k)
            break
            print(record.header._d["warc-trec-id"])
            for i in tuple_k:
                f_out.writelines(str((record.header._d["warc-trec-id"],i[0],i[1]))+'\n')

    f.close()
    f_out.close()
