import shutil
import requests
from pprint import pprint, pformat
from ieee80211EsSubmitter import Ieee80211EsSubmitter
from ieee80211RepoApi import Ieee80211RepoApi
import json

if __name__ == '__main__':
    es_submitter = Ieee80211EsSubmitter()
    repoapi = Ieee80211RepoApi()
    #print(es_submitter.factory_reset_ES())
    #local_filename="11-21-1013-00-00bi-tgbi-teleconference-minutes-17-june-2021.docx"
    #wufiles = [x for x in os.listdir() if x.split('.')[-1] in es_submitter.fileextset ]  
    metadata_dict = repoapi.read_metadata_file(repoapi.DEFAULT_TOTAL_METADATA_FILENAME)
    print(f'{len(metadata_dict["repo_entries"])} read.')

    knownbad = [
            ['11-03-0226-00-0wng-ls-to-3gpp-sa2.doc', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-03-0118-04-000i-alternate-text-for-tgi-8-3-4.doc', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-03-0063-01-000e-lb51-comment-resolution-clause-7.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-09-0940-00-000u-lb156-comment-spreadsheet.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-09-0909-00-000s-sf-rfi-wednesday.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-09-0471-11-000s-lb-147-comment-resolutions.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-09-0829-00-000s-resolutions-for-approval.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-02-0328-00-000i-1x-pre-authentication.ppt', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-01-0182-00-000g-summary-statement-of-supergold-proposal-to-tgg.ppt', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-06-0537-27-000r-d2-comments.xls', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-05-0967-02-0jtc-wapi-posiion-paper.ppt', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"],
            ['11-04-0991-01-000s-w-chamb-ppt.ppt', "RequestError(400, 'parse_exception', 'Error parsing document in field [_ingest._value.b64data]')"]
            ]
    kbfns = [x[0] for x in knownbad]
    retries = [ x for x in metadata_dict['repo_entries'] if x['doc_url'].split('/')[-1] in kbfns ]
    # pprint(retries)
    failed_entries = []
    for entry in retries:
        doc_url = entry['doc_url']
        local_filename = doc_url.split('/')[-1]

        # Download and index:
        print(f'{local_filename} downloading . . .')
        with requests.get(f'{repoapi.docrepo_root}{doc_url}', stream=True) as r:
            with open(local_filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print(f'{local_filename} downloaded.')
        try:
            es_submitter.localfile_to_esindex(local_filename = local_filename)
        except Exception as e:
            print(f'Error indexing {local_filename}.')
            failed_entries.append([local_filename, pformat(e)])

    #for ff in failed_entries:
    #    print(f'{ff[0]}\n{ff[1]}')
    #    pprint(ff[1])

    #errs = [ x[1] for x in failed_entries ]
    #for e in errs:
    #    pprint(e)

    #print(failed_entries)
    #pprint(failed_entries)
    fej = json.dumps(failed_entries)
    print(fej)

    #es_submitter.print_all_docs_all_indices()
    print('Done.')

