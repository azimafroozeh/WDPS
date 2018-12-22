from bs4 import BeautifulSoup
import en_core_web_sm
nlp = en_core_web_sm.load()
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
def extract_currency_relations(doc):
    # merge entities and noun chunks into one token
    spans = list(doc.ents) + list(doc.noun_chunks)
    for span in spans:
        span.merge()

    relations = []
    for entity in doc:
        if entity.dep_ in ('attr', 'dobj'):
            subject = [w for w in entity.head.lefts if w.dep_ == 'nsubj']
            if subject:
                subject = subject[0]
                relations.append((subject, entity))
        elif entity.dep_ == 'pobj' and entity.head.dep_ == 'prep':
            relations.append((entity.head.head, entity))
    return relations
def html_to_string(record):
    url = record.url
    text = None

    if url:
        payload = record.payload.read()
    #print(len(nlp.vocab))

    #key, text = record
    html = payload
    # print(html)
    soup = BeautifulSoup(html, 'html5lib')
    for script in soup(['head', 'title', '[document]', "script", "style", 'aside']):
        script.extract()
    # print(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    # print("===================================")
    article = nlp(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    # if len(html) == 0:
    #     return 0
    # #print(html)
    # soup = BeautifulSoup(html, 'html5lib')
    # for script in soup(['head', 'title', 'meta', '[document]',"script", "style", 'aside']):
    #     script.extract()
    # #print(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    # #print("===================================")
    # #return soup.get_text()
    # article = nlp(" ".join(re.split(r'[\n\t]+', soup.get_text())))
    #print(article)
    #print(article)
    #print("articcccccccccccccccccccccccccle")
    for x in soup.get_text().split('.'):
        # print(type(x))
        #print(x)
        x=nlp(x)
        relations = extract_currency_relations(x)
        print(relations)
        try:
            relations = extract_currency_relations(x)
            for r1, r2 in relations:
                if r2.ent_type_:
                    yield (r1.text, r2.ent_type_, r2.text)
        except:
            pass
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
            # if '27' not in record.header._d["warc-trec-id"]:
            #     continue

            tuple_k=html_to_string(record)
            #print(tuple_k)
            print(record.header._d["warc-trec-id"])
            for i in tuple_k:
                print(i)
                f_out.writelines(str((record.header._d["warc-trec-id"],i[0],i[1],i[2]))+'\n')

    f.close()
    f_out.close()
