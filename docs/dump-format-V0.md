# Data Dump Binary format
Due to historical reasons, the dumper was built to dump the data in a very specific binary format. This can and will change in the future, ideally to a standardized format like protobuf. However, for now, this document records the binary format that the files are dumped with, so that any populator can be written to understand this format.

## Key dump
The key dump is just the output of `lru_crawler metadump <all/hash>` which is basically a single key and its associated metadata per line.

## Data dump
The Data dump enters the following for each memcached key/value pair. (NO spaces between fields; NO delimiters between KV pairs)

***<keylen (2-bytes)> <key> <expiry (4-bytes)> <flag (4-bytes)> <datalen (4-bytes)> <data>***

Until a populator is available as part of this project, writing any application that can read the following binary format and writing it to memcached would suffice. See MemcachedUtils::CraftMetadataString() for the writer part.