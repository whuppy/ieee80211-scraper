import os
import sys
import yaml
import boto3
import requests
import json
import pprint
import base64
from datetime import datetime
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import traceback

pp = pprint.PrettyPrinter(indent=2)

class Ieee80211EsSubmitter:
    
    def __init__(self):
        self.es_index = os.environ['ES_INDEX']
        self.bucketname = os.environ['S3_BUCKET']
        self.aws_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_key_secret = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_region = os.environ['AWS_REGION']
        # File exts and their frequencies in total repo as of 2021-08-18:
        # {'docx': 19373, 'pptx': 16555, 'ppt': 15559, 'doc': 14625, 'xls': 6077, 
        # 'xlsx': 2823, 'pdf': 329, 'zip': 135, 'vsd': 42, 'odt': 17, 'dot': 17, 
        # 'rtf': 13, 'potx': 8, 'docm': 8, 'csv': 5, 'vsdx': 4, 'odp': 4, 'ods': 4, 
        # 'txt': 4, 'xlsm': 3, 'pot': 2, 'pptm': 2, 'mpg': 2, 'potm': 1, 'doc0': 1, 
        # 'bin': 1, 'deploy': 1, 'eml': 1, 'pps': 1, 'html': 1}
        self.fileextset = ['doc', 'docx', 'ppt', 'pptx', 'xls', 'xlsx', 'pdf']

        self.config_filename = os.environ['YAML_FILENAME']
        with open(self.config_filename, 'r') as file_desc:
            try:
                self.config = yaml.safe_load(file_desc)
            except:
                print("File load yaml config file", config_file)
                print('Error: {}'.format(str(traceback.format_exc())))
                sys.exit(1)
        
        self.s3 = boto3.client("s3")
        self.s3bucket = boto3.resource("s3").Bucket(self.bucketname)
        self.bucket_keys = set( [ b.key for b in self.s3bucket.objects.all() ] )
        self.bucket_list = [ x for x in sorted(self.bucket_keys) if x.split(".")[-1] in self.fileextset ]
        #print(f'{len(self.bucket_list)} indexable items in bucket {self.bucketname}.')

        if self.config['es']['useHttpAuth']:
            self.ES_auth = [os.environ['ES_MASTER_USERNAME'],
                            os.environ['ES_MASTER_PASSWORD']]
        else:
            self.ES_auth = None
        self.es_doctype = self.config['es']['type']
        self.es_max_submission_size = self.config['es']['size_limit']
        self.es_client = Elasticsearch(hosts=[{'host': self.config['es']['host'], 
                                               'port': self.config['es']['port']}],
                                       http_auth=self.ES_auth,
                                       use_ssl=True,
                                       verify_certs=True,
                                       connection_class=RequestsHttpConnection)
                                  
        # From Tenyo:
        tenyo_ingest_pipeline = {
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
        self.es_client.ingest.put_pipeline(id="attachment", body=tenyo_ingest_pipeline)
        mjs_ingest_pipeline = {
            "description" : "MS Office Document Pipeline",
            "processors" : [{
                        "attachment" : { "field" : "b64data", "target_field" : "attachment" },
                        "remove" : { "field" : "b64data" }
            }]
        }
        self.es_client.ingest.put_pipeline(id="mjs-attachment", body=mjs_ingest_pipeline)
        #pp.pprint(self.es_client.ingest.get_pipeline(id='*'))
        #print("Initialized.")

    def factory_reset_ES(self):
        '''
        Nuke and pave ElasticSearch index.
        '''
        print(f'Performing factory reset on {self.es_index} . . .')
        if self.es_client.indices.exists(self.es_index):
            print(self.es_client.indices.delete(index=self.es_index, ignore=[400, 404]))
            print(f'Index {self.es_index} deleted.')
        else:
            print(f'Index {self.es_index} not found.')

        with open(self.config['es']['mapping_filename'], "r") as f:
            es_mapping = json.load(f)
        print(self.es_client.indices.create(index=self.es_index, body=es_mapping))
        print(f'Created index {self.es_index}.')
        allrslt = self.es_client.search(index=self.es_index,_source=False,size=10000,request_timeout=100)
        pp.pprint(allrslt)
        for r in allrslt['hits']['hits']:
            #pp.pprint(r)
            print(f'_index={r["_index"]}, _id={r["_id"]}')
        print("Factory reset complete.")
        
    def localfile_to_esindex(self, repo_entry, pline = "attachment"):
        '''
        Takes a .docx/.ppt/etc. file.
        If file is not already indexed, read, base64, and upload it to the ElasticSearch index.
        The es_client.create() will throw exceptions like 503 and parse errors,
        so call this function in a try/except.
        I think the exceptions are best handled a level up from here.
        '''
        # Check to see if file is already present in the index:
        local_filename = repo_entry['doc_url'].split('/')[-1]
        rslt = self.es_client.get(index=self.es_index, id=local_filename,
                                  request_timeout=100, ignore=[400, 404])
        if rslt['found']:
            print(f'{local_filename} already indexed. Skipping . . .')
        else:
            with open(local_filename, "rb") as local_file:
                local_encoded = base64.b64encode(local_file.read()).decode("utf-8")
                # Generates a serialization error without the utf-8 decode
                #local_encoded = base64.b64encode(local_file.read())
            print(f'Base64 encoding of {local_filename} is {len(local_encoded)} bytes long.')
            if ( len(local_encoded) > self.es_max_submission_size ) :
                print(f'Maximum submission size {self.es_max_submission_size} exceeded.')
                # Just submit the metadata:
                metadata_only_es_body = repo_entry
                metadata_only_es_body["time_indexed"] = datetime.now()
                print(f'Submitting metadata only for {local_filename} to ES index "{self.es_index}" . . .' )
                rslt = self.es_client.index(index=self.es_index, id=local_filename,
                                             body=metadata_only_es_body, 
                                             request_timeout=100)
                print(rslt['result'])
                # If it failed it would have thrown an exception. So we can assume success here.
                print(f'Metadata for {local_filename} indexed.')
            else:
                # Submit metadata and original document:
                es_body = repo_entry
                es_body["time_indexed"] = datetime.now()
                es_body["attachments"] = [ {"filename" : local_filename, "b64data" : local_encoded} ]
                print(f'Submitting {local_filename} to ES index "{self.es_index}" . . .' )
                rslt = self.es_client.index(index=self.es_index, id=local_filename,
                                             body=es_body, 
                                             pipeline=pline, 
                                             request_timeout=100)
                print(rslt['result'])
                # If it failed it would have thrown an exception. So we can assume success here.
                print(f'{local_filename} indexed.')
        
        return(rslt)
        
    def bucketkey_to_esindex(self, repo_entry):
        '''
        Uses localfile_to_esindex() if keyname is not indexed. Otherwise skips.
        Does not handle any exceptions thrown by localfile_to_esindex(),
        as i think it's best handled one more level up by the caller.

        Deprecated because the current method grabs the file directly from repo to local and then 
        does the indexing.
        '''
        # Check to see if keyname is already present in the index:
        keyname = entry['doc_url'].split('/')[-1]
        rslt = self.es_client.get(index=self.es_index, id=keyname,
                                  request_timeout=100, ignore=[400, 404])
        if rslt['found']:
            print(f'{keyname} already indexed. Skipping download and indexing . . .')
        else:
            self.s3.download_file(self.bucketname, keyname, keyname)
            # I think the exceptions are best handled a level up from here.
            # So I'm not putting this in a try/except:
            self.localfile_to_esindex(repo_entry)
            # Although if an exception is thrown, the local file won't be removed.
            # So be sure to handle that in the caller's exception handler.
            os.remove(keyname)

    def most_recent_entry(self):
        #result = self.es_client.search(index=self.es_index,_source=False,sort="oreated_date:desc",size=1,request_timeout=100)
        result = self.es_client.search(index=self.es_index,_source=False,request_timeout=100)
        #result = self.es_client.search(index=self.es_index,sort="_source.oreated_date:desc",size=1,request_timeout=100)
        #print(result['hits']['hits'][0])
        docid = result['hits']['hits'][0]['_id']
        #doc = self.es_client.get(_source=False,index=self.es_index,id=docid)
        doc = self.es_client.get(index=self.es_index,id=docid)
        return doc

    def print_all_docs_all_indices(self):        
        allrslt = self.es_client.search(_source=False,size=10000,request_timeout=100)
        print("All docs in all indices:")
        #pp.pprint(allrslt)
        for r in allrslt['hits']['hits']:
            #pp.pprint(r)
            print(f'_index={r["_index"]}, _id={r["_id"]}')
        
if __name__ == '__main__':
    print('Null main. Done.')
