# WDPS

https://bitbucket.org/AzimAfroozeh/vuwebdata/


# Usage
##Step1:run cluster_run.sh
- Input: default-"hdfs:///user/bbkruit/sample.warc.gz"
- Output: "result" (WARC-File-ID,Label, Freebase-ID)
- Configuration: 8 executors; 2G/Ex; Cluster mode; Virtual Environment
##Step2:run Query&Evaluate.sh
- Query input: "result" (Produced by cluster_run.sh)
- Query output: "result.tsv" (Processed by transformer.py)
- Annnotaion File: default-"data/sample.annotations.tsv"
#Techniques
## Entity Extraction
- Environment: Spark(PySpark) & Virtual Environment(path:~/anaconda3/envs/nltk_env)
- Text Extraction: BeautifulSoap exclusive ['head', 'title', '[document]',"script", "style", 'aside']
- Named Entity Recognition (NER): en_core_web_sm(Spacy) - offline traind model
## Entity Linking
- Corpus: Freebase accessing by elasticsearch
- Filter rules: exclusive non-English words, starting with space or number, containing punctuations
- Ranking rule: Top score given by elasticsearch