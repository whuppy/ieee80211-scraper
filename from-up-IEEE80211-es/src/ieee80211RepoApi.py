#!/usr/bin/env python
# -*- coding=utf-8 -*-

import requests
from bs4 import BeautifulSoup
import yaml
import boto3
import datetime
import json
import os
import shutil
import logging
from ieee80211EsSubmitter import Ieee80211EsSubmitter
import pprint

class Ieee80211RepoApi:
    DEFAULT_METADATA_FILE = 'metadata.json'
    DEFAULT_TOTAL_METADATA_FILENAME = 'total-repo-metadata.json'
    DEFAULT_ENTRIES_FILE = 'entries-dicts.json'
    # Currently assuming CWD is below where config.yaml is:
    DEFAULT_CONFIG_FILE = '../config.yaml'
    
    def __init__(self):
        self.config_filename = self.DEFAULT_CONFIG_FILE
        with open(self.config_filename, 'r') as file_desc:
            try:
                self.config = yaml.safe_load(file_desc)
            except:
                print("File load yaml config file", self.config_filename)
                print('Error: {}'.format(str(traceback.format_exc())))
                sys.exit(1)

        self.repo_url = self.config['ieee80211']['document_root']
        self.docrepo_root = self.config['ieee80211']['docrepo_root']
        self.copy_to_s3 = self.config['ieee80211']['copy_to_s3']
        self.keep_local_copies = self.config['ieee80211']['keep_local_copies']
        #self.num_pages = self.config['ieee80211']['num_pages']
        self.num_pages = self.determine_num_pages_in_repo()
        
        self.bucketname = os.environ['S3_BUCKET']
        self.aws_key_id = os.environ['AWS_ACCESS_KEY_ID']
        self.aws_key_secret = os.environ['AWS_SECRET_ACCESS_KEY']
        self.aws_region = os.environ['AWS_REGION']

        self.start_page = 1
        self.last_page  = self.num_pages

        self.s3 = boto3.client("s3")
        self.s3bucket = boto3.resource("s3").Bucket(self.bucketname)
        self.bucket_keys = set( [ b.key for b in self.s3bucket.objects.all() ] )
        self.file_exts_seen = []

        self.total_metadata_dict = self.update_total_metadata()
        
    def set_start_and_last(self, start_page, last_page):
        self.start_page = start_page
        self.last_page = last_page
        
    def determine_num_pages_in_repo(self):
        '''
        Input an impossibly large page number and see what actually gets returned.
        Be careful, the IEEE site master changed the desired class name to
        "selected num want_sep", and might change it again.
        '''
        curr_page = 98765
        args = {'n': '%d' % curr_page}
        page = requests.get(self.repo_url, params=args)
        soup = BeautifulSoup(page.content, 'html.parser')
        # Find the page number of the page actually returned:
        args = { "class" : "selected num want_sep"}
        returned_pagenum = soup.find('span', attrs = args).contents[0]
        requested_page = soup.find('form')['action'].split('=')[1] 
        #print(f'Requested page {requested_page}, received page {returned_pagenum}.')
        return int(returned_pagenum)
    
    def create_dict_from_datarow(self, datarow):
        # datarow is a bs4.element.Tag
        all_fields = datarow.find_all('td')
        datarow_dict = {}
        try:
            # CREATED, DCN_YEAR, DCN_DCN, DCN_REV, WGROUP, TITLE, AUTH_AFFIL, UPLOAD_DATE, URL
            datarow_dict['created_date']    = all_fields[0].contents[0].contents[0]
            datarow_dict['dcn_year']   = all_fields[1].contents[0]
            datarow_dict['dcn_num']    = all_fields[2].contents[0]
            datarow_dict['dcn_rev']    = all_fields[3].contents[0]
            datarow_dict['wgroup']     = all_fields[4].contents[0]
            datarow_dict['title']      = all_fields[5].contents[0]
            datarow_dict['auth_affil'] = all_fields[6].contents[0]
            datarow_dict['author'] = datarow_dict['auth_affil'].split('(')[0]
            try:
                datarow_dict['affiliation'] = datarow_dict['auth_affil'].split('(')[1][:-1]
            except:
                print(f"Inner exception, cannot split {datarow_dict['auth_affil']}, assigning auth_affil value.")
                datarow_dict['affiliation'] = datarow_dict['auth_affil']
            datarow_dict['upload_date'] = all_fields[7].contents[0].contents[0]
            datarow_dict['doc_url']     = all_fields[8].contents[0]['href'] 
        except : 
            print(f"Outer exception, auth_affil = {datarow_dict['auth_affil']}, punting on the whole row.") 
            return None 
        return datarow_dict 

    def retrieve_docurls_from_repo(self):
        '''
        Retrieve just the naked URLs and none of the metadata from the repo. 
        Deprecated.
        '''
        urllist = []
        for pagenum in range(self.start_page, self.last_page + 1):
            args = {'n': '%d' % pagenum}
            page = requests.get(self.repo_url, params=args)
            contents = page.content
            print(f'Parsing document URLs from {page.url} . . .')
            soup = BeautifulSoup(contents, 'html.parser')
            args = { "class" : "b_data_row"}
            for row in soup.find_all('tr', attrs = args ):
                all_fields = row.find_all('td')
                href = all_fields[8].contents[0]['href']
                # e.g. https://mentor.ieee.org/802.11/dcn/21/11-21-0587-03-0000-2021-may-wg11-agenda.xlsx
                urllist.append(f'{self.docrepo_root}{href}')
        return urllist
                  
    def write_metadata_to_file(self, all_repo_entries, filename = DEFAULT_METADATA_FILE , copy_to_s3 = False):
        '''
        Take all the scraped repo entries and write them out into the metadata file.
        '''
        metadata_dict = {}
        metadata_dict['created_date']   = datetime.datetime.now().isoformat()
        metadata_dict['source_url']     = self.repo_url
        metadata_dict['doc_prefix']     = self.docrepo_root
        metadata_dict['num_entries']    = len(all_repo_entries)
        metadata_dict['num_pages']      = self.num_pages
        metadata_dict['start_page']     = self.start_page
        metadata_dict['last_page']      = self.last_page
        metadata_dict['repo_entries']   = all_repo_entries
        metadata_dict['file_exts_seen'] = self.file_exts_seen
        # Make sure to *not* open as binary.
        with open(filename,'w') as fout: 
            json.dump(metadata_dict, fout)
        if copy_to_s3:
            print(f'{filename} uploading to {self.bucketname} . . .')
            # s3.upload_fileobj is expecting bytes, not a string.
            with open(filename, 'rb') as f:
                self.s3.upload_fileobj(f, Bucket=self.bucketname, Key=filename)
            print(f'{filename} uploaded to {self.bucketname}.')

    def scrape_repo_metadata(self, mdfilename = DEFAULT_METADATA_FILE):
        '''
        Scrape a selected range of the repo's metadata into a JSON file (default name
        entries-dicts.json) and put it in the bucket.

        Set start and last page of range with set_start_and_last() before calling this.
        '''
        filextset = set()

        # Hand-craft some JSON out into entries-dicts.json file.
        entries_file = open(self.DEFAULT_ENTRIES_FILE,'w')
        entries_file.write('[ \n')

        # Handle every web page from start_page to last_page
        first_row = True
        for pagenum in range(self.start_page, self.last_page + 1):
            args = {'n': '%d' % pagenum}
            page = requests.get(self.repo_url, params=args)
            contents = page.content
            timestamp = datetime.datetime.now().isoformat()
            print(f'{timestamp}: Parsing metadata from {page.url} . . .')
            soup = BeautifulSoup(contents, 'html.parser')
            args = { "class" : "b_data_row"}
            # Handle every entry from the web page.
            for row in soup.find_all('tr', attrs = args ):
                rowdict = self.create_dict_from_datarow(row) 
                href = rowdict['doc_url']
                filename = href.split('/')[-1]
                fileext = filename.split('.')[-1]
                filextset.add(fileext)
                # Write out the metadata file entry for the rowdict:
                rowdictstring = json.dumps(rowdict)
                if first_row:
                    entries_file.write(f'{rowdictstring}\n')
                    first_row = False
                else:
                    entries_file.write(f',\n{rowdictstring}')

        # Finish up and close entries-dicts.json file:
        entries_file.write("\n ]\n")
        entries_file.close()

        # Create metadata.json from entries-dicts.json file:
        self.file_exts_seen = sorted(filextset)
        self.write_metadata_to_file(all_repo_entries = self.read_metadata_file(self.DEFAULT_ENTRIES_FILE), filename=mdfilename)

    def scrape_all_metadata(self, mdfilename = DEFAULT_TOTAL_METADATA_FILENAME):
        '''
        Scrape all the repo's metadata into a JSON file and put it in the bucket.
        '''
        filextset = set()

        # Hand-craft some JSON out into a file.
        entries_file = open(self.DEFAULT_ENTRIES_FILE,'w')
        entries_file.write('[ \n')

        # Handle every web page from 1 to num_pages + 1:
        first_row = True
        for pagenum in range(1, self.num_pages + 1):
            args = {'n': '%d' % pagenum}
            page = requests.get(self.repo_url, params=args)
            contents = page.content
            timestamp = datetime.datetime.now().isoformat()
            print(f'{timestamp}: Parsing metadata from {page.url} . . .')
            soup = BeautifulSoup(contents, 'html.parser')
            args = { "class" : "b_data_row"}
            # Handle every entry from the web page.
            for row in soup.find_all('tr', attrs = args ):
                rowdict = self.create_dict_from_datarow(row) 
                href = rowdict['doc_url']
                filename = href.split('/')[-1]
                fileext = filename.split('.')[-1]
                filextset.add(fileext)
                # Write out the metadata file entry for the rowdict:
                rowdictstring = json.dumps(rowdict)
                if first_row:
                    entries_file.write(f'{rowdictstring}\n')
                    first_row = False
                else:
                    entries_file.write(f',\n{rowdictstring}')

        # Finish up and close entries JSON file:
        entries_file.write("\n ]\n")
        entries_file.close()

        # Create metadata JSON from entries file:
        self.file_exts_seen = sorted(filextset)
        self.write_metadata_to_file(all_repo_entries = self.read_metadata_file(self.DEFAULT_ENTRIES_FILE), filename=mdfilename)

    def metadata_for_bucketkey(self, keyname):
        '''
        assumes bucket keynames are unique
        does not check if keyname was found in metadata
        '''
        rents = self.total_metadata_dict['repo_entries']
        meda = [ x for x in rents if keyname in x['doc_url'] ]
        return meda[0]

    def read_metadata_file(self, filename = DEFAULT_METADATA_FILE):
        with open(filename, 'r') as read_file:
            metadata_dict = json.load(read_file)
        return metadata_dict 
                  
    def update_total_metadata(self, metadata_filename=DEFAULT_TOTAL_METADATA_FILENAME):
        '''
        Pull the total metadata.json file from the bucket.
        Determine last page of repo accessed from metadata.json.
        Compare to the current size of the repo.
        
        If an entry is not in metadata_dict['repo_entries'], then append.
        Write out updated metadata.json and upload it to the bucket.

        Since the entries are in reverse chronological order, 
        i.e. the newest entries are always on the first page,
        always scrape the first page since it may have gotten new
        entries without creating a new page.
        '''
        # Download total metadata.json from bucket. Stomps on local copy.
        if (metadata_filename in self.bucket_keys):
            print(f'Downloading {metadata_filename} from S3 bucket . . .')
            self.s3.download_file(self.bucketname, metadata_filename, metadata_filename)
        else:
            # Need to create total metadata file from scratch:
            print(f'Total metadata file {metadata_filename} not found in bucket {self.bucketname}, creating from scratch . . .')
            self.scrape_all_metadata(mdfilename=metadata_filename)
        metadata_dict = self.read_metadata_file(metadata_filename)

        # If there are new pages to scrape, scrape their metadata and update the
        # total metadata.json file and upload it to the bucket.
        if (self.num_pages > metadata_dict['num_pages']):
            # Scrape the unscraped pages and append the in-memory dict from the entries file:
            self.file_exts_seen = metadata_dict['file_exts_seen']
            # The repo is in reverse chron order, so the new entries are up front:
            # That "+ 1" at the end is so that the first page is always scraped.
            self.set_start_and_last(start_page = 1, last_page = self.num_pages - metadata_dict['num_pages'] + 1)
            print(f'Repo updated since last total metadata file, scraping {self.num_pages - metadata_dict["num_pages"]} new pages . . .')
            self.scrape_repo_metadata()
            # Use the entries-dicts.json file created by scrape_repo_metadata()
            new_entries = self.read_metadata_file(self.DEFAULT_ENTRIES_FILE)
            for e in new_entries:
                if e in metadata_dict['repo_entries']:
                    print(f'Already in dict {e}, skipping . . .')
                else:
                    print(f'Appending {e} to dict . . .')
                    metadata_dict['repo_entries'].append(e)
            # Update the rest of the in-memory dict, write and upload it:
            metadata_dict['created_date']   = datetime.datetime.now().isoformat()
            metadata_dict['num_entries']    = len(metadata_dict['repo_entries'])
            metadata_dict['num_pages']      = self.num_pages
            metadata_dict['start_page']     = self.start_page
            metadata_dict['last_page']      = self.last_page
            metadata_dict['file_exts_seen'] = self.file_exts_seen
            # Make sure to *not* open as binary.
            with open(metadata_filename,'w') as fout: 
                json.dump(metadata_dict, fout)
            print(f'Uploading {metadata_filename} to {self.bucketname} . . .')
            # Copy updated local total metadata file back to bucket.
            # s3.upload_fileobj is expecting bytes, not a string.
            with open(metadata_filename, 'rb') as f:
                self.s3.upload_fileobj(f, Bucket=self.bucketname, Key=metadata_filename)
            print(f'Uploaded {metadata_filename} to {self.bucketname}.')
        else:
            print(f'Total metadata file {metadata_filename} up to date.')


        # Return the updated dict:
        return metadata_dict

    def metadata_guided_indexing(self, metadata_filename=DEFAULT_TOTAL_METADATA_FILENAME, copy_to_s3=False):
        '''
        Similar to lo_mem_site_scrape(), below, but driven by the metadata file.
        Update the total metadata file and then go through it.

        Keeps track of failures and returns tracebacks for failed entries.
        '''
        if ( metadata_filename == self.DEFAULT_TOTAL_METADATA_FILENAME ) :
            metadata_dict = self.total_metadata_dict
        else:
            metadata_dict = self.read_metadata_file(filename = metadata_filename)

        esclient = Ieee80211EsSubmitter()
        failed_entries = []
        # TODO: sort repo_entries in reverse chron just to be sure.
        for entry in metadata_dict['repo_entries']:
            # Check to see if file is already present in the index:
            doc_url = entry['doc_url']
            local_filename = doc_url.split('/')[-1]
            rslt = esclient.es_client.get(index=esclient.es_index, id=local_filename,
                                      request_timeout=100, ignore=[400, 404])
            if rslt['found']:
                print(f'Already indexed {local_filename}. Skipping . . .')
            else:
                # Download and index:
                print(f'{local_filename} downloading . . .')
                with requests.get(f'{self.docrepo_root}{doc_url}', stream=True) as r:
                    with open(local_filename, 'wb') as f:
                        shutil.copyfileobj(r.raw, f)
                print(f'{local_filename} downloaded.')
                try: 
                    esclient.localfile_to_esindex(entry)
                except Exception as e:
                    print(f'Error indexing {local_filename}.')
                    failed_entries.append([local_filename, str(pprint.pformat(e))])

            # Put the file in the bucket if it isn't there already:
            if copy_to_s3:
                if (local_filename in self.bucket_keys):
                    print(f'File already in bucket: {local_filename}')
                else:
                    if os.path.isfile(local_filename):
                        print(f'Local file exists: {local_filename}, skipping download.')
                    else:
                        print(f'Downloading {local_filename} . . .')
                        with requests.get(f'{self.docrepo_root}{href}', stream=True) as r:
                            with open(local_filename, 'wb') as f:
                                shutil.copyfileobj(r.raw, f)
                        print(f'{local_filename} downloaded.')
                    print(f'{local_filename} uploading to {self.bucketname} . . .')
                    with open(local_filename, 'rb') as f:
                        self.s3.upload_fileobj(f, Bucket=self.bucketname, Key=local_filename)
                    print(f'{local_filename} uploaded to {self.bucketname}.')
            else:
                pass

            # Clean up local file:
            if os.path.isfile(local_filename):
                os.remove(local_filename)
                print(f'{local_filename} local copy removed.')

        return failed_entries

    def lo_mem_site_scrape(self, keep_local_copies=False, copy_to_s3=False):
        '''
        Go through the site page by page,
        which means 100 documents at a time.
        Write all the entries out to a file as JSON dicts, 
        and handcraft some commas and brackets to make it loadable JSON.
        Download file to local, upload to s3 as per need and preference.
        Generate the metadata file using the handcrafted JSON file,
        and write metadata file out locally and to the bucket.
        '''
        filextset = set()

        # Hand-craft some JSON out into a file.
        entries_file = open(self.DEFAULT_ENTRIES_FILE,'w')
        entries_file.write('[ \n')

        # Handle every web page from start_page to last_page:
        first_row = True
        for pagenum in range(self.start_page, self.last_page + 1):
            args = {'n': '%d' % pagenum}
            page = requests.get(self.repo_url, params=args)
            contents = page.content
            timestamp = datetime.datetime.now().isoformat()
            print(f'{timestamp}: Parsing metadata from {page.url} . . .')
            soup = BeautifulSoup(contents, 'html.parser')
            args = { "class" : "b_data_row"}

            # Handle every entry from the web page.
            for row in soup.find_all('tr', attrs = args ):
                rowdict = self.create_dict_from_datarow(row) 
                href = rowdict['doc_url']
                filename = href.split('/')[-1]
                fileext = filename.split('.')[-1]
                filextset.add(fileext)
                # Write out the metadata file entry for the rowdict:
                #rowdict['size'] = filesize # oops need to download the file to get this info
                rowdictstring = json.dumps(rowdict)
                if first_row:
                    entries_file.write(f'{rowdictstring}\n')
                    first_row = False
                else:
                    entries_file.write(f',\n{rowdictstring}')

                # Quick short-circuit for most common circumstance:
                if filename in self.bucket_keys and not keep_local_copies:
                    print(f'{filename} found in {self.bucketname} and keep_local_copies==False, continuing . . .')
                    continue

                if os.path.isfile(filename):
                    print(f'{filename} exists, skipping download.')
                else:
                    print(f'{filename} downloading . . .')
                    with requests.get(f'{self.docrepo_root}{href}', stream=True) as r:
                        with open(filename, 'wb') as f:
                            shutil.copyfileobj(r.raw, f)
                    print(f'{filename} downloaded.')

                filesize = os.stat(filename).st_size

                # Upload if wanted and needed:
                if filename in self.bucket_keys:
                    print(f'{filename} found in {self.bucketname}, skipping upload.')
                else:
                    if copy_to_s3:
                        print(f'{filename} uploading to {self.bucketname} . . .')
                        with open(filename, 'rb') as f:
                            self.s3.upload_fileobj(f, Bucket=self.bucketname, Key=filename)
                        print(f'{filename} uploaded to {self.bucketname}.')
                    else:
                      print(f'{filename} copy_to_s3 set to False, not uploading.')

                # Clean up if wanted:
                if not keep_local_copies:
                    os.remove(filename)
                    print(f'{filename} local copy removed.')
        
        # Finish up and close entries JSON file:
        entries_file.write("\n ]\n")
        entries_file.close()

        # Create metadata.json from entries file:
        self.file_exts_seen = sorted(filextset)
        self.write_metadata_to_file(all_repo_entries = self.read_metadata_file(self.DEFAULT_ENTRIES_FILE))


if __name__ == '__main__':
    print('Null main. Done.')
