# es_transfer
```sh
# Copy all source data + map to destination
./es_transfer -fi="source ip" -ti="dest ip" 

# Do not use source index mapping
./es_transfer -fi="source ip" -ti="dest ip" -km=true

# Specify a single or multiple index, Multiple indexes separated by commas
./es_transfer -fi="source ip" -ti="dest ip" -i="index1,index2,index3"

# Copy source index mapping only, do not import data
./es_transfer -fi="source ip" -ti="dest ip" -num=-99

# Single shard copy 3000 ，No data will be copied if -num is less than - bnum
./es_transfer -fi="source ip" -ti="dest ip" -num=3000

# Read 2 indexes in parallel
./es_transfer -fi="source ip" -ti="dest ip" -p=2

# The data is processed by specifying the preprocessing pipeline ID of the target es
./es_transfer -fi="source ip" -ti="dest ip" -pid=test1

```

``` json 
PUT _ingest/pipeline/test1
{
  "description": "this is test pipeline",
  "processors": [
    {
      "drop": {
        "if": " ctx?.tags == null "
      }
    },
    {
      "set": {
        "field": "_index",
        "value": "test_{{tags}}"
      }
    }
  ]
}
```
