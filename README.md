A simple performance comparison for RocksJava which
shows the difference between writing to the database
using the 3 different available methods:

1. Direct Puts, i.e. calling `org.rocksdb.RocksDB#put(cf, key, value);`

2. Using a Write Batch

3. Using a Write Batch With Index

The tool allows you to provide a serialized dump of column families where each
file in the folder is a column family. The tool will then try and store the
data from each column family into RocksDB using each of the methods described
above.

Column Family dump file format
------------------------------
The format of a serialized column dump file is basically a sequential stream
of records, where each record has the format:

```
[type (1 byte) | keyLen (2 bytes) | key (keyLen bytes) | valueLen (4 bytes) | value (valueLen bytes)]
```

The `type` is either:

* `1` = `PUT`
* `2` = `MERGE`
* `4` = `DELETE`
* `8` = `LOG`


Results
-------
Current performance comparison looks like:

```
Benchmarking with: 2730192 key/value pairs

Direct puts took: 6421ms

2730192 entries in the WriteBatch
Writing the WriteBatch took: 489ms
WriteBatch puts took: 1651ms

2730192 entries in the WriteBatchWithIndex
Writing the WriteBatchWithIndex took: 547ms
WriteBatchWithIndex puts took: 4155ms
```
