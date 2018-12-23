## INFORM: this implementation was not tested on DAS becouse of the [issue](https://canvas.vu.nl/courses/36660/discussion_topics/117109)
## Application Usage
### Prerequisite: source virtual environment(not tested)
- source ~/.bashrc
- source activate entk_env
### Run:

- Run: `Run_relation_extraction.sh`

### Output:

- File: `relations_result`
- Format: `<WARC-File-ID,A,TypeOf B,B>`
## Techniques
- Lib: Spacy, en_core_web_sm
- Patterns:
- Pattern 1: nsubj [\*] (attr|dobj):"Lymphoma is one more cancer types that populace get."=> (Lymphoma, one more cancer types)
- Pattern 2: [\*] perp pobj:"was more deserving of a nomination than 95\% of the field."=> (nomination,95\%)
