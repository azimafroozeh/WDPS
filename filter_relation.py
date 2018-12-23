import re
def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        print('wait')
        return False
    else:
        return True
def processing(word):
    #word=word.encode('ascii', 'ignore').decode('unicode_escape')
    word=' '.join(word.split())
    # p = re.compile(r"([ ])(\1+)")
    # word=p.sub('\1',word)
    # word=word.replace('\n','').replace('\t','')
    return word
if __name__ == '__main__':
    import sys

    try:
        _, INPUT,OUTPUT = sys.argv
        #print(OUTPUT)
    except Exception as e:
        print('Usage: python starter-code.py INPUT')
        sys.exit(0)
    with open(INPUT) as f:
        text=f.readlines()
    lines=[eval(i) for i in text]

    result=[]
    for line in lines:
        #if isEnglish(line[1]) and isEnglish(line[3]):
        result.append((line[0],processing(line[1]),line[2],processing(line[3])))
    f_out=open(OUTPUT,'w')
    for i in result:
        f_out.writelines(str(i) + '\n')