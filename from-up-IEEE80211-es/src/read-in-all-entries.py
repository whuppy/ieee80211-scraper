from ieee80211RepoApi import Ieee80211RepoApi
import json

wally = Ieee80211RepoApi()
repo_entries = wally.read_metadata_file(filename = "../rundir/entries-dicts.json")
print(len(repo_entries))

print("Opening entire repo metadata . . .")
with open('bucket-metadata.json', 'r') as read_file:
    metadata_dict = json.load(read_file)
filenames = [ u['doc_url'].split("/")[-1] for u in metadata_dict['repo_entries'] ]                         
filextset = set( [ v.split(".")[-1] for v in filenames] )
print(filextset)
working_group_set = set( [u['wgroup'] for u in metadata_dict['repo_entries']] )
print(working_group_set)

