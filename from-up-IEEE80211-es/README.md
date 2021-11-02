# IEEE80211-es

Prepare data for IEEE 802.11 ElasticSearch instance

## Reserved environment variables

- `ES_URL` : ElasticSearch endpoint
- `ES_INDEX` : ElasticSearch index name
- `S3_BUCKET` : S3 bucket name
- `AWS_KEY_ID`
- `AWS_KEY_SECRET`
- `AWS_REGION`

## Todo list

### Phase 1: index data

- [X] Download all IEEE 802.11 documents 
- [X] Upload documents to AWS S3
- [ ] Extract text from documents
- [X] Define ElasticSearch mapping
- [ ] index metadata and full text into ElasticSearch

### Phase 2: Keep data up to date

This cronjob will be scheduled once a week.

We will use the following command to run this task: `python src/update.py`

- [ ] Download the latest IEEE 802.11 documents and upload to S3
- [ ] index metadata and full text into ElasticSearch
- [ ] Write a Dockerfile
