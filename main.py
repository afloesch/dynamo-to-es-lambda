from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from datetime import datetime
import boto3
import os
import json

'''
event_example = {
  'Records': [
    {'eventID': '6c2a22c58c3d429e62fb429ee66dd22f', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1', 'dynamodb': {'ApproximateCreationDateTime': 1593054844.0, 'Keys': {'date': {'S': '2020-06-25T03:13:46.115Z'}}, 'NewImage': {'date': {'S': '2020-06-25T03:13:46.115Z'}, 'num': {'N': '2'}, 'type': {'S': 'log'}}, 'SequenceNumber': '8600000000024412923535', 'SizeBytes': 68, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:us-east-1:417834917721:table/log-test/stream/2020-06-25T03:04:11.744'},
    {'eventID': 'e2da289fdd26bdc52ab2c761f843f9ab', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1', 'dynamodb': {'ApproximateCreationDateTime': 1593231332.0, 'Keys': {'date': {'S': '2020-06-27T04:12:37.577Z'}}, 'NewImage': {'arr': {'L': [{'S': 'string value'}, {'N': '3'}, {'S': 'test string'}]}, 'date': {'S': '2020-06-27T04:12:37.577Z'}, 'boolfield': {'BOOL': True}, 'numset': {'NS': ['2', '1']}, 'num': {'N': '26'}, 'dict': {'M': {'key2': {'N': '2'}, 'key': {'S': 'value'}}}, 'stringarray': {'SS': ['test', 'test2', 'test3']}}, 'SequenceNumber': '9562400000000049227109512', 'SizeBytes': 163, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:us-east-1:417834917721:table/log-test/stream/2020-06-25T03:04:11.744'},
    {'eventID': '087a2353bcb652b600ff54cb858e29d1', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1', 'dynamodb': {'ApproximateCreationDateTime': 1593233767.0, 'Keys': {'date': {'S': '2020-06-27T04:53:07.164Z'}}, 'NewImage': {'date': {'S': '2020-06-27T04:53:07.164Z'}, 'maptest2': {'M': {'str': {'S': 'string value'}, 'nestedlist': {'L': [{'N': '1'}, {'S': 'test string'}, {'N': '2'}, {'BOOL': True}]}, 'num': {'N': '1'}}}, 'num': {'N': '2'}}, 'SequenceNumber': '9562500000000049228611693', 'SizeBytes': 127, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:us-east-1:417834917721:table/log-test/stream/2020-06-25T03:04:11.744'},
    {'eventID': '00fb7c0168c2d87762d7091aba7fee18', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1', 'dynamodb': {'ApproximateCreationDateTime': 1593238465.0, 'Keys': {'date': {'S': '2020-06-27T06:13:10.113Z'}}, 'NewImage': {'date': {'S': '2020-06-27T06:13:10.113Z'}, 'maptest': {'M': {'key1': {'S': 'value1'}, 'nestedlist': {'L': [{'M': {'key2': {'S': 'value2'}, 'key3': {'N': '1'}}}]}}}}, 'SequenceNumber': '9562600000000049231534096', 'SizeBytes': 113, 'StreamViewType': 'NEW_IMAGE'}, 'eventSourceARN': 'arn:aws:dynamodb:us-east-1:417834917721:table/log-test/stream/2020-06-25T03:04:11.744'}
  ]
}
'''

def parse_primitive(data):

  # Parse a given Dynamo event for one of the below basic types and return 
  # the first match

  if 'S' in data:
    return data['S']

  if 'N' in data:
    return data['N']

  if 'BOOL' in data:
    return str(data['BOOL'])

  if 'SS' in data:
    return data['SS']

  if 'NS' in data:
    return data['NS']


def parse_list(list):

  # Parse a dynamo list object, calling parse_key on each item in the list to 
  # handle nested maps or lists

  parsed = []

  for i in list:
    d = parse_key(i)

    if d is not None:
      parsed.append(d)

  return parsed


def parse_key(data):

  # Parse a Dynamo event object key for the various supported Dynamo data types

  # key is a map object
  if 'M' in data:
    d = walk_keys(data['M'])
    if d is not None:
      return d

  # key is a list object
  if 'L' in data:
    d = parse_list(data['L'])
    if d is not None:
      return d

  # key is some other kind of primitive type
  d = parse_primitive(data)
  if d is not None:
    return d


def walk_keys(record):

  # Walk every key in a dynamo event recursively to build a properly formatted 
  # dict which can be json stringified and indexed in elasticsearch
  
  keys = record.keys()
  final = {}

  for i in keys:
    data = record[i]
    d = parse_key(data)

    if d is not None:
      final[i] = d

  return final


def lambda_handler(event, context):

  host = os.getenv('HOST')
  region = os.getenv('REGION')
  records = event['Records']
  now = datetime.utcnow()
  indexName = os.getenv('INDEX_NAME') + "-{0}-{1}-{2}".format(now.month, now.day, now.year)
  index = json.dumps({
    'index': {
      '_index': indexName,
    }
  })
  bulk = ""

  for r in records:
    if 'eventName' not in r or r['eventName'] != 'INSERT' or 'dynamodb' not in r:
      continue

    data = r['dynamodb']
    f = walk_keys(data['NewImage'])
    if f is not None:
      bulk = bulk + index + "\n" + json.dumps(f) + "\n"

  if bulk == "":
    return

  credentials = boto3.Session().get_credentials()
  awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)
    
  es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
  )

  es.bulk(bulk, indexName)
