import pandas as pd
import json
import glob
import uuid
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from io import StringIO
import numpy
# get data file names
path =r'./input/airlines'
filenames = glob.glob(path + "/DAL.csv")

dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename, converters={'CRSDepTime': '{:0>4}'.format,'CRSArrTime': '{:0>4}'.format}))

aircraft_performance = pd.concat(dfs, ignore_index=True)
d = aircraft_performance.to_json(orient='index')

es = Elasticsearch()

records = aircraft_performance.to_dict(orient='records')
for j in range(len(records)):
    print(json.dumps(records[j]))
    es.index(index="my-index", body=json.dumps(records[j]))


#es = Elasticsearch()
#es.indices.create(index='my-index', ignore=400)
#es.index(index="my-index", id=42, body=f"json")
#helpers.bulk(es, doc_generator(aircraft_performance))
