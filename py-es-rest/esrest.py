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
import base64

class ElasticRest:
    '''
    Use python's web interface instead of curl to access ElasticSearch.
    It seems kind of silly to me now to have a whole separate class as a wrapper.

    This is still pretty specific to the IEEE802.11 repo scaper implementation.
    '''

    def __init__(self):
        self.es_url  = os.environ['ES_URL']
        self.es_index = os.environ['ES_INDEX']

        self.es_user     = os.environ['ES_MASTER_USERNAME']
        self.es_password = os.environ['ES_MASTER_PASSWORD']
        userpass = (self.es_user + ":" + self.es_password).encode("utf-8")
        self.authtoken = base64.b64encode(userpass)
        self.authheader = "Authorization: Basic " + self.authtoken.decode("utf-8")

    def create_index(self, index_name=None, data=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name
        headers = {"Content-Type" : "application/json"}
        result = requests.put(url, data=data, headers=headers)
        #print(result.text)
        return result
    
    def get_index(self, index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name
        result = requests.get(url)
        return result.json()

    def delete_index(self, index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name
        result = requests.delete(url)
        return result

    def index_exists(self, index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name
        result = requests.head(url)
        return (200 == result.status_code)

    def put_pipeline(self, pipeline_name=None, pipeline_data=None):
        if (None == pipeline_name):
            pipeline_name = "attachment"
        if (None == pipeline_data):
            pipeline_data = {
                "description": "Extract attachment information from arrays",
                "processors": [
                    {
                        "foreach": {
                            "field": "attachments",
                            "processor": {
                                "attachment": {
                                    "target_field": "_ingest._value.attachment",
                                    "field": "_ingest._value.b64data"
                                }
                            }
                        }
                    },
                    {
                        "foreach": {
                            "field": "attachments",
                            "processor": {
                                "remove": {
                                    "field": "_ingest._value.b64data"
                                }
                            }
                        }
                    }
                ]
            }
        url = self.es_url + "_ingest/pipeline/" + pipeline_name
        result = requests.put(url, data=pipeline_data)
        return result

    def factory_reset(self, index_name=None, data=None):
        if (None == index_name):
            index_name = self.es_index
        if self.index_exists(index_name):
            self.delete_index(index_name)
        if (None == data):
            data = '''
                {
                    "settings": {
                        "index": {
                            "analysis": {
                                "normalizer": {
                                    "custom_normalizer_lowercase": {
                                        "type": "custom",
                                        "filter": "lowercase"
                                    }
                                }
                            }
                        }
                    },
                    "mappings": {
                        "properties": {
                            "wgroup": { "type": "keyword", "normalizer": "custom_normalizer_lowercase" },
                            "author": { "type": "keyword", "normalizer": "custom_normalizer_lowercase" },
                            "affiliation": { "type": "keyword", "normalizer": "custom_normalizer_lowercase" },
                            "created_date": { "type": "date", "format": "dd-LLL-yyyy zz" },
                            "upload_date": { "type": "date", "format": "dd-LLL-yyyy HH:mm:ss zz" },
                            "dcn_year": { "type": "integer" },
                            "dcn_num": { "type": "integer" },
                            "dcn_rev": { "type": "integer" },
                            "title": { "enabled": "false" },
                            "doc_url": { "enabled": "false" }
                        }
                    }
                }
            '''

        result = self.create_index(index_name)
        return result

    def search_index(self, query='', index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + '/_search'
        headers = {"Content-Type" : "application/json"}
        result = requests.get(url, data=query, headers=headers)
        return result.json()

    def most_recent_docs(self, index_name=None):
        '''
        Retrieve sorted by document created_date in descending order.
        '''
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + '/_search'
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

    print(er.authheader)
    print(er.factory_reset())
    print(er.index_exists())

    exit(1)

    result = er.index_exists(er.es_index)
    pp.pprint(result.status_code)

    result = er.index_exists("bogus")
    pp.pprint(result.status_code)


    url = er.es_url + er.es_index + "/_stats/docs"
    result = requests.get(url)
    pprint.pprint(result.json())

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

