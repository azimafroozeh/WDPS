#!/usr/bin/env bash
python local_extract_mention.py data/sample.warc.gz relations
python filter_relation.py relations relations_result