#!/usr/bin/python
#
# Run the ES REST interface via the web.
# Basically using python instead of curl.
#curl -X PUT "https://search-open-hugkeqllxa5lm2g36arrt7nbhe.us-west-1.es.amazonaws.com/jctvc" -H 'Content-Type: application/json' --data-binary "@mapping.json"

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
        return result
    
    def get_index(self, index_name=None):
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name
        result = requests.get(url)
        return result

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
        return (200 == requests.head(url).status_code)

    def put_doc(self, doc_id, doc_data, index_name=None, pipeline_name=None):
        '''
        Put a document in the index.
        '''
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + "/_create/" + doc_id
        if (None == pipeline_name):
            pipeline_name = 'attachment'
        options = {'pipeline' : pipeline_name}
        headers = {"Content-Type" : "application/json"}
        result = requests.put(url, data=json.dumps(doc_data), params=options, headers=headers)
        return result

    def get_doc(self, doc_id, index_name=None):
        '''
        Retrieve a document from the index.
        '''
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
        '''
        Delete and re-create the index.
        '''
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
                            "title": { "type": "keyword", "normalizer": "custom_normalizer_lowercase" },
                            "doc_url": { "enabled": "false" }
                        }
                    }
                }
            '''
        result = self.create_index(index_name, data=data)
        return result

    def search_index(self, query='', index_name=None):
        '''
        Uses the _search interface, which is different from getting a doc.
        '''
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + '/_search'
        headers = {"Content-Type" : "application/json"}
        result = requests.get(url, data=query, headers=headers)
        return result

    def most_recent_docs(self, index_name=None):
        '''
        Retrieve sorted by document created_date in descending order.
        This has been tested to work properly in both asc and desc order.
        '''
        if (None == index_name):
            index_name = self.es_index
        url = self.es_url + index_name + '/_search'
        headers = {"Content-Type" : "application/json"}
        query = '''
        {
            "sort" : [ { "created_date" : "desc" } ],
            "query" : { "match_all" : {} }
        }
        '''
        result = requests.get(url, data=query, headers=headers)
        return result.json()

    def localfile_to_esindex(self, repo_entry):
        '''
        Read in a local file and submit it to index.
        '''
        local_filename = entry['doc_url'].split('/')[-1]
        result = er.get_doc(local_filename)
        if (200 == result.status_code):
            print(f'{local_filename} already in index {er.es_index}, skipping . . .')
        else:
            print(f'{local_filename} not in index {er.es_index}, downloading and indexing . . .')
            es_body = entry
            with requests.get(f'{docrepo_root}{entry["doc_url"]}', stream=True) as r:
                with open(local_filename, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
            with open(local_filename, "rb") as local_file:
                local_encoded = base64.b64encode(local_file.read()).decode("utf-8")
            es_body["attachments"] = [ {"filename" : local_filename, "b64data" : local_encoded} ]

            #for some reason this doesn't work as serial json,
            # maybe cast it into a string? :
            es_body["time_indexed"] = str(datetime.now())
            # huh that seems to work

            result = er.put_doc(doc_id=local_filename, doc_data=es_body)
            os.remove(local_filename)

        return result

if __name__ == '__main__':
    pp = pprint.PrettyPrinter(indent=1)
    er = ElasticRest()
    docrepo_root = 'https://mentor.ieee.org/'

    #print(er.factory_reset().text)

    # Read in a small example set of metadata:
    with open('smol_meda.json', 'r') as read_file:
        metadata_dict = json.load(read_file)
    # Add them to index:
    for entry in metadata_dict['repo_entries']:
        er.localfile_to_esindex(entry)

    # Get stats on the index:
    pprint.pprint(requests.get(er.es_url + er.es_index + "/_stats/docs").json())

    exit(1)
    ###############

    print(er.authheader)
    print(er.factory_reset())
    print(er.index_exists())
    pp.pprint(er.get_index())

    result = er.index_exists("bogus")
    pp.pprint(result.status_code)

    # This works.
    #pp.pprint(er.most_recent_docs())

    # This works:
    query_sort_by_title = '''
    {
        "sort"  : [ { "title" : "desc" } ],
        "query" : { "match_all" : {} }
    }
    '''
    pp.pprint(er.search_index(query_sort_by_title).json())
    
    # Retrieve one of the docs from the index:
    doc_entry = metadata_dict['repo_entries'][4]
    doc_id = doc_entry['doc_url'].split('/')[-1]
    pp.pprint(er.get_doc(doc_id=doc_id).json())


