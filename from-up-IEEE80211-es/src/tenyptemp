from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
import json
import os
import base64


def es_recreate(es_url, es_index, es_mapping):
    # reindex if mapping or settings changed
    print("ES: Recreating ...")
    es = Elasticsearch(
        [es_url],
        port=443,
        use_ssl=True,
        http_auth=AWS4Auth(
            os.environ.get('AWS_KEY_ID'),
            os.environ.get('AWS_KEY_SECRET'),
            os.environ.get('AWS_REGION'),
            'es'
        ),
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=200
    )
    es.indices.create(index=es_index, body=es_mapping)
    return es


def init_ingest_pipeline(es):
    # init ingest pipeline
    ingest_body = {
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
    es.ingest.put_pipeline(id="attachment", body=ingest_body)


if __name__ == '__main__':
    es_url = os.environ["ES_URL"]
    es_index = "ieee80211"
    with open("src/mapping.json", "r") as f:
        es_mapping = json.load(f)
    es = es_recreate(es_url, es_index, es_mapping)
    init_ingest_pipeline(es)

    with open("NOTES.txt", "rb") as f:
        b64data = base64.b64encode(f.read()).decode("utf-8")

    body = {
        "title": "test data",
        "attachments": [{
            "filename": "NOTES.txt",
            "b64data": b64data}]
    }
    es.index(index=es_index, id="1", pipeline="attachment", body=body, request_timeout=100)
