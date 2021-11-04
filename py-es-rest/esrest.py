#!/usr/bin/python
#
# Run the ES REST interface via the web.
# Basically using python instead of curl.

#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jctvc" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jctvc" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jctvc" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jvt" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-jct-vc-3adhpezogrrffz6vs4zhdwruhi.us-west-1.es.amazonaws.com/meeting" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-jct-vc-3adhpezogrrffz6vs4zhdwruhi.us-west-1.es.amazonaws.com/jvt" -H 'Content-Type: application/json' --data-binary "@mapping.json"

import os
import requests
import json
import pprint


class ElasticRest:
    '''
    Use python's web interface instead of curl to access ElasticSearch.
    It seems kind of silly to me now to have a whole separate class as a wrapper.
    '''

    def __init__(self):
        self.es_url  = os.environ['ES_URL']
        self.es_index = os.environ['ES_INDEX']
        #self.bucketname = os.environ['S3_BUCKET']
        #self.aws_key_id     = os.environ['AWS_ACCESS_KEY_ID']
        #self.aws_key_secret = os.environ['AWS_SECRET_ACCESS_KEY']
        #self.aws_region     = os.environ['AWS_REGION']
        pass

    def factory_reset(self):
        pass

    def get_index(self):
        url = self.es_url + self.es_index
        result = requests.get(url)
        return result.json()

    def search_index(self, query=''):
        url = self.es_url + self.es_index + '/_search'
        headers = {"Content-Type" : "application/json"}
        result = requests.get(url, data=query, headers=headers)
        return result.json()

    def most_recent_docs(self):
        '''
        Retrieve sorted by document created_date in descending order.
        '''
        url = self.es_url + self.es_index + '/_search'
        headers = {"Content-Type" : "application/json"}
        query = '''
        {
            "sort" : [
                { "created_date" : "desc" }
            ],
            "query" : {
                "match_all" : {}
            }
        }
        '''
        result = requests.get(url, data=query, headers=headers)
        return result.json()


if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=1)
    er = ElasticRest()

    result = er.get_index()
    pp.pprint(result)

    url = er.es_url + er.es_index + "/_stats/docs"
    result = requests.get(url)
    pprint.pprint(result.json())
    exit(1)

    pp.pprint(er.most_recent_docs())

    query2 = '''
    {
        "sort" : [
            { "created_date" : "desc" }
        ],
        "query" : {
            "term" : {
                "dcn_year" : "2021"
           }
        }
    }
    '''
    pp.pprint(er.search_index(query2))

