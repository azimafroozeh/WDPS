def filter(line):
    tag=eval(line[2])
    #tag=eval(tag)
    if tag:
        #if line[1] in tag[0]:
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
