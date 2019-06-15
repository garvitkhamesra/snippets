from elasticsearch import helpers, Elasticsearch
import csv

es = Elasticsearch()

with open('/home/garvit/Downloads/site_ref.csv') as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index='region', doc_type='report')