# DynamoDB to Elasticsearch Lambda

Generic Python 3.8 lambda code to process DynamoDB event stream and bulk upload the events to Elasticsearch. Recursively parses dynamo lists and maps into a nested and valid JSON document for Elastic. Treats all primitive types as strings to avoid potential elasticsearch indexing errors due to mismatched types in array values.

Values other than maps list or strings can be indexed using an elasticsearch template.

## Env 

| VAR | Description |
|---|---|
| HOST | Elastic search hostname |
| INDEX_NAME | Elastic search index name.Example index format: INDEX_NAME-MM-DD-YYYY |
| REGION | AWS region |

## Build

```
python3 -m venv venv
. venv/bin/activate
make build
```