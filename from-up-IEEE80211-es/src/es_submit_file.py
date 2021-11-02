import base64
from pprint import pprint
from ieee80211EsSubmitter import Ieee80211EsSubmitter

submitter = Ieee80211EsSubmitter()
submitter.factory_reset_ES()

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
#submitter.es_client.ingest.put_pipeline(id="dogsnot", body=tenyo_ingest_pipeline)
pprint(submitter.es_client.ingest.get_pipeline(id='*'))
submitter.localfile_to_esindex(local_filename='../NOTES.txt', pline='attachment')
submitter.localfile_to_esindex(local_filename='example.doc',  pline='attachment')

