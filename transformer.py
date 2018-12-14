import re
import string
def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True
def filter(line):
    tag=eval(line[2])
    #tag=eval(tag)
    if tag:
        for p in string.punctuation:
            if p in line[1]:
                return None
        if not line[1].startswith(' ') and not line[1][0].isdigit():
            if line[1].lower() in tag[0].lower() and isEnglish(line[1]):
                return line[0]+'\t'+line[1]+'\t'+tag[1]
if __name__ == '__main__':
    import sys

    try:
        INPUT = sys.argv[1]
        OUTPUT = sys.argv[2]
    except Exception as e:
        sys.exit(0)
    with open(INPUT, 'r') as f:
        lines = f.readlines()
        lines = map(lambda x: eval(x), lines)
        lines = map(lambda x: filter(x), lines)
        with open(OUTPUT, 'w') as f2:
            for i in lines:
                if i:
                    f2.write(i + '\n')
