# WDPS - Knowledge Aquisition

This repository contains the Lab Assignment of the 2018-2019 edition of the  Vrije Universiteit Amsterdam, Computer Science Master Degree, Web Data Processing System course.

You can find the old project in the archived Bitbucket [repository](https://bitbucket.org/AzimAfroozeh/vuwebdata/). Due to the high storage space required by the environment and configuration files, we decided to create a new repository with a simplified and smaller version of the project.

## Application Usage

Working space: `/home/wdps1810/wdps-test`

### Step1: Knowledge Extraction

Task: This performs (1) NLP preprocessing and (2) information extraction. This will return a set of entities present in the sample file, with its respective Named Entity Recognition (NER) tag.

Run:

- Run: `run.sh cluster.py "hdfs:///user/bbkruit/sample.warc.gz"`
- Change third argument to your HDFS file

Output:

- File: `result`
- Format: `<WARC-File-ID,Label,NER Tag>`

Configuration:

- 8 executors;
- 2G/Ex;
- Cluster mode;
- Virtual Environment.

### Step2: Result Evaluation

Task: This takes the results produced in the previous step and performs entity linking in the knowledge base. The returned result is a set of rows with the linking of the entry samples in the knowledge base. Note that the result is processed in `transformer.py`.

Run:

- File: `Query&Evaluate.sh`
- Parameters: `"result"`

Output:

- File: "result.tsv"
- Format: `<WARC-File-ID,Label,Freebase-ID>`

Annotation File: `default-"data/sample.annotations.tsv"` for a testing score of other datasets, you can change the annotation file

## Techniques

### Entity Extraction

- Environment:
  - Spark(PySpark)
  - Virtual Environment (path: `~/anaconda3/envs/nltk_env`)
- Text Extraction: 
  - [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) exclusive ['head', 'title', '[document]',"script", "style", 'aside']
- NER:
  - en_core_web_sm ([Spacy](https://spacy.io/)), an offline trainned model

### Entity Linking

- Corpus:
  - Freebase accessing by elasticsearch
- Filter rules:
  - Remove non-English words.
  - Remove words starting with space or numbers.
  - Remove words containing punctuations.
- Ranking rule:
  - Top score given by elasticsearch
  
  ## Scalability 
  - First part of our solution is completely scalabe, and the WARC file splits to 100 containers.
