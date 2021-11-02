#!/usr/bin/env python
# -*- coding=utf-8 -*-
# Called by cron once a week.

import requests
import boto3
import yaml
import datetime
import json
import os
import shutil
from ieee80211RepoApi import Ieee80211RepoApi
from ieee80211EsSubmitter import Ieee80211EsSubmitter
from pprint import pprint

if __name__ == '__main__':
    print(f'Update begun     {datetime.datetime.now().isoformat()}')
    repo_scraper = Ieee80211RepoApi()
    print(f'{repo_scraper.num_pages} pages in IEEE 802.11 document repo.')
    failed_entries = repo_scraper.metadata_guided_indexing(copy_to_s3=False)
    pprint(failed_entries)
    for ent in failed_entries:
        print(ent)
    print(f'Update completed {datetime.datetime.now().isoformat()}')
     # Make sure to *not* open as binary.
    with open("failed-entries.json",'w') as fout:
        json.dump(failed_entries, fout)

