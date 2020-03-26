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
```
