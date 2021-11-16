# Cachemover
----------
Memcached servers in a cloud environment also undergo hardware degradation, maintenance downtime, etc. This project provides the ability to dump memcached data (all KV pairs) to disk, and populate a memcached process on a different server.

This is used as part of the EVCache project at Netflix, for cluster maintenance and
scaling. Read more about how its used in this technical article (TODO: Add Medium link).

It has 2 components:
1. Cache dumper
2. Cache populator (Under development. Refer [docs/dump-format-V0.md](./docs/dump-format-V0.md) for the disk format to write your own)

The dumper component of this project can be configured using the options below to
point to a memcached process that's running (using the `ip` and `port` configs), and
dumps its data to `output_dir`.

With the populator process (*still under development*), it can be used to read the data files from disk and re-populate another memcached process.

Refer [this document](./docs/architecture.md) for a High Level Architecture breakdown.

----------

### Build instructions:

From the project root directory, create a new directory called build, and run cmake from there:
```bash
$> ./scripts/build_scripts/install_deps.sh  (sudo if necessary)
$> mkdir build
$> cd build/
$> cmake ..
$> make
```

To build with GPROF:
```bash
...
$> cmake -DGPROF_ENABLED=1 ..
$> make
```

To build with ASAN:
```bash
...
$> cmake -DASAN_ENABLED=1 ..
$> make
```

This will create a bin/ directory under the root project directory.

### Usage
----------

To run the cache dumper, pass in the appropriate arguments:
```bash
$> cd ../bin/
$> ./memcache-dumper -h
Memcached dumper options
Usage: ./bin/memcache-dumper [OPTIONS]

Options:
  -h,--help                           Print this help message and exit
  --config_file_path TEXT REQUIRED    YAML configuration file path.
```
```bash
Configuration file options:
REQUIRED:
  ip                      STRING        Memcached IP.
  port                    UINT          Memcached port.
  threads                 UINT          Num. threads
  bufsize                 UINT          Size of single memory buffer (in bytes).
  memlimit                UINT          Maximum allowable memory usage (in bytes).
  key_file_size           UINT          The maximum size for each key file (in bytes).
  data_file_size          UINT          The maximum size for each date file (in bytes).
  log_file_path           STRING        Desired log file path.
  output_dir              STRING        Desired output directory path.
  req_id                  STRING        Request ID to identify current run

OPTIONAL:
  bulk_get_threshold      UINT          Number of keys to bulk get. (Default = 30)
  only_expire_after_s     UINT          Only dump keys that expire after these many seconds. (Default = 0)
  checkpoint_resume       BOOLEAN       Resume dump from previous incomplete run. (Default = false)
  is_s3_dump              BOOLEAN       Upload dumped files to S3 if true. (Default = false)
  s3_bucket               STRING        S3 Bucket name
  s3_final_path           STRING        Path under S3 bucket to dump to
  sqs_queue               STRING        SQS Queue name to notify on file upload
  ketama_bucket_size      UINT          Bucket size to use for Ketama hashing
  all_ips                 LIST<STRING>  List of all IPs in target replica as strings
  dest_ips                LIST<STRING>  List of destination IPs in target replica

```
An example configuration file can be found under `test/test_config.yaml`
