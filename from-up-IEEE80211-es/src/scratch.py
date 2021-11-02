import os
import json
from pprint import pprint
from ieee80211EsSubmitter import Ieee80211EsSubmitter
from ieee80211RepoApi import Ieee80211RepoApi

es_submitter = Ieee80211EsSubmitter()
repo_scraper = Ieee80211RepoApi()

print(f'Total metadata file size: {len(repo_scraper.total_metadata_dict["repo_entries"])}')
pprint(repo_scraper.metadata_for_bucketkey("11-21-1638-01-00bi-october-telecons-agenda.pptx"))
failures = repo_scraper.metadata_guided_indexing(metadata_filename="smol_meda.json")
pprint(failures)
result = es_submitter.most_recent_entry()
pprint(result)

#pprint(es_submitter.most_recent_entry())

exit(0)
#############
#repo_scraper.metadata_guided_indexing(copy_to_s3=False)
with open("failures.json", "r") as read_file:
    failures = read_file.read()
pprint(failures)

exit(0)
#############

mdd = repo_scraper.update_total_metadata()

all_exts = [ x["doc_url"].split('/')[-1].split('.')[-1] for x in mdd["repo_entries"] ]
#print(all_exts)
fext_count = {}
for fext in all_exts:
    if fext in fext_count.keys():
        fext_count[fext] = fext_count[fext] + 1
    else:
        fext_count[fext] = 1
sorted_fext_counts = dict(sorted(fext_count.items(), key = lambda item: item[1], reverse = True))
#print(sorted_fext_counts)
sorted_fexts = sorted_fext_counts.keys()
print(sorted_fexts)

all_groups = set([ x["wgroup"] for x in mdd["repo_entries"] ])
print(sorted(all_groups))

exit(0)
#############

#with open("../rundir/total-repo-metadata.json", 'r') as read_file:
with open("../rundir/metadata.json", 'r') as read_file:
    metadata_dict = json.load(read_file)

with open("../rundir/entries-dicts.json", 'r') as read_file:
    entries_dicts = json.load(read_file)

for e in entries_dicts:
    if e in metadata_dict['repo_entries']:
        print(f'already exists {e["title"]} ')
    else:
        print(f'not found {e["title"]} Adding . . .')
        metadata_dict['repo_entries'].append(e)

print('we aint writing out nothing yet. bailing . . .')
exit(0)


all_exts = [ x["doc_url"].split('/')[-1].split('.')[-1] for x in metadata_dict["repo_entries"] ]
#print(all_exts)

fext_count = {}
for fext in all_exts:
    if fext in fext_count.keys():
        fext_count[fext] = fext_count[fext] + 1
    else:
        fext_count[fext] = 1

#pprint(fext_count)

sorted_fext_counts = dict(sorted(fext_count.items(), key = lambda item: item[1], reverse = True))
#print(sorted_fext_counts)
sorted_fexts = sorted_fext_counts.keys()
print(sorted_fexts)

all_groups = set([ x["wgroup"] for x in  metadata_dict["repo_entries"] ])
print(sorted(all_groups))

es_submitter.factory_reset_ES()
