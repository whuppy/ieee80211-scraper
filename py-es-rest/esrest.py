#!/usr/bin/python
#
# Run the ES REST interface via the web.
# Basically using python instead of curl.

#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jctvc" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jvt" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-jct-vc-3adhpezogrrffz6vs4zhdwruhi.us-west-1.es.amazonaws.com/meeting" -H 'Content-Type: application/json' --data-binary "@mapping.json"
#curl -X PUT "https://search-jct-vc-3adhpezogrrffz6vs4zhdwruhi.us-west-1.es.amazonaws.com/jvt" -H 'Content-Type: application/json' --data-binary "@mapping.json"

import os
import shutil
from datetime import datetime
import requests
import json
import pprint
import base64

class ElasticRest:
    '''
    Use python's web interface instead of curl to access ElasticSearch.
    It seems kind of silly to me now to have a whole separate class as a wrapper.

    This is still pretty specific to the IEEE802.11 repo scraper implementation.
    '''

    def __init__(self):
        self.es_url   = os.environ['ES_URL']
        self.es_index = os.environ['ES_INDEX']

        # Create basic HTTP authorization header:
        self.es_user     = os.environ['ES_MASTER_USERNAME']
        self.es_password = os.environ['ES_MASTER_PASSWORD']
        userpass = (self.es_user + ":" + self.es_password).encode("utf-8")
        self.authtoken = base64.b64encode(userpass)
        self.authheader = { "Authorization" : "Basic " + self.authtoken.decode("utf-8") }


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

    def put_doc(self, doc_id, doc_data, index_name=None, pipeline_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + "/_create/" + doc_id
        if (None == pipeline_name):
            pipeline_name = 'attachment'
        options = {'pipeline' : pipeline_name}
        headers = {"Content-Type" : "application/json"}
        #print(doc_data)
        result = requests.put(url, data=json.dumps(doc_data), params=options, headers=headers)
        print(result.url, result.text)
        return result

    def get_doc(self, doc_id, index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + "/_doc/" + doc_id
        result = requests.get(url)
        return result

    def put_pipeline(self, pipeline_name=None, pipeline_data=None):
        if (None == pipeline_name):
            pipeline_name = "attachment"
        if (None == pipeline_data):
            # Tenyo's pipeline:
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
            # Mappings for IEEE802.11 repo:
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
        result = self.create_index(index_name, data=data)
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
    docrepo_root = 'https://mentor.ieee.org/'

    #print(er.factory_reset().text)

    with open('smol_meda.json', 'r') as read_file:
        metadata_dict = json.load(read_file)
    for entry in metadata_dict['repo_entries']:
        local_filename = entry['doc_url'].split('/')[-1]
        result = er.get_doc(local_filename)
        if (200 == result.status_code):
            print(f'{local_filename} already in index {er.es_index}, skipping . . .')
        else:
            print(f'{local_filename} not in index {er.es_index}, downloading and indexing . . .')
            with requests.get(f'{docrepo_root}{entry["doc_url"]}', stream=True) as r:
                with open(local_filename, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            with open(local_filename, "rb") as local_file:
                local_encoded = base64.b64encode(local_file.read()).decode("utf-8")
            es_body = entry
            #es_body["time_indexed"] = datetime.now()
            es_body["attachments"] = [ {"filename" : local_filename, "b64data" : local_encoded} ]
            result = er.put_doc(doc_id=local_filename, doc_data=es_body)

    url = er.es_url + er.es_index + "/_stats/docs"
    result = requests.get(url)
    pprint.pprint(result.json())

    pp.pprint(er.most_recent_docs())

    exit(1)
    
    print(er.authheader)
    print(er.factory_reset())
    print(er.index_exists())
    pp.pprint(er.get_index())


    result = er.index_exists(er.es_index)
    pp.pprint(result.status_code)

    result = er.index_exists("bogus")
    pp.pprint(result.status_code)



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

