# Native Memcached dumper

**Note: Still under development.**

- Build instructions:

From the project root directory, create a new directory called build, and run cmake from there:
```
$> ./scripts/build_scripts/install_deps.sh  (sudo if necessary)
$> mkdir build
$> cd build/
$> cmake ..
$> make
```

To build with GPROF:
```
...
$> cmake -DGPROF_ENABLED=1 ..
$> make
```

To build with ASAN:
```
...
$> cmake -DASAN_ENABLED=1 ..
$> make
```

This will create a bin/ directory under the root project directory. To run, pass in the appropriate arguments:
```
$> cd ../bin/
$> ./memcache-dumper -h
Memcached dumper options
Usage: ./bin/memcache-dumper [OPTIONS]

Options:
  -h,--help                   Print this help message and exit
  --config_file_path TEXT REQUIRED
                              Configuration file path.

Configuration file options:
  ip TEXT REQUIRED                      Memcached IP.
  port INT REQUIRED                     Memcached port.
  threads INT REQUIRED                  Num. threads
  bufsize UINT REQUIRED                 Size of single memory buffer (in bytes).
  memlimit UINT REQUIRED                Maximum allowable memory usage (in bytes).
  key_file_size UINT REQUIRED           The maximum size for each key file (in bytes).
  data_file_size UINT REQUIRED          The maximum size for each date file (in bytes).
  log_file_path TEXT REQUIRED           Desired log file path.
  output_dir TEXT REQUIRED              Desired output directory path.
  req_id TEXT REQUIRED                  Request ID to identify current run
  bulk_get_threshold INT                Number of keys to bulk get.
  only_expire_after_s INT               Only dump keys that expire after these many seconds.
  checkpoint_resume BOOLEAN             Resume dump from previous incomplete run.
  is_s3_dump BOOLEAN                    Upload dumped files to S3 if true
  s3_bucket BOOLEAN                     S3 Bucket name
  s3_final_path BOOLEAN                 Path under S3 bucket to dump to
  sqs_queue BOOLEAN                     SQS Queue name to notify on file upload
  ketama_bucket_size UINT               Bucket size to use for Ketama hashing
  all_ips TEXT                          List of all IPs in target replica as strings
  dest_ips TEXT                         List of destination IPs in target replica

An example configuration file can be found under `test/test_config.yaml`
```
