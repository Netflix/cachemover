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
Usage: ../bin/memcache-dumper [OPTIONS] ip port threads bufsize memlimit key_file_size data_file_size log_file_path output_dir [bulk_get_threshold] [only_expire_after_s] [checkpoint_resume]

Positionals:
  ip TEXT REQUIRED                      Memcached IP.
  port INT REQUIRED                     Memcached port.
  threads INT REQUIRED                  Num. threads
  bufsize UINT REQUIRED                 Size of single memory buffer (in bytes).
  memlimit UINT REQUIRED                Maximum allowable memory usage (in bytes).
  key_file_size UINT REQUIRED           The maximum size for each key file (in bytes).
  data_file_size UINT REQUIRED          The maximum size for each date file (in bytes).
  log_file_path TEXT REQUIRED           Desired log file path.
  output_dir TEXT REQUIRED              Desired output directory path.
  bulk_get_threshold INT                Number of keys to bulk get.
  only_expire_after_s INT               Only dump keys that expire after these many seconds.
  checkpoint_resume BOOLEAN             Resume dump from previous incomplete run.

Options:
  -h,--help                             Print this help message and exit
  -i,--ip TEXT REQUIRED                 Memcached IP.
  -p,--port INT REQUIRED                Memcached port.
  -t,--threads INT REQUIRED             Num. threads
  -b,--bufsize UINT REQUIRED            Size of single memory buffer (in bytes).
  -m,--memlimit UINT REQUIRED           Maximum allowable memory usage (in bytes).
  -k,--key_file_size UINT REQUIRED      The maximum size for each key file (in bytes).
  -d,--data_file_size UINT REQUIRED     The maximum size for each date file (in bytes).
  -l,--log_file_path TEXT REQUIRED      Desired log file path.
  -o,--output_dir TEXT REQUIRED         Desired output directory path.
  -g,--bulk_get_threshold INT           Number of keys to bulk get.
  -e,--only_expire_after_s INT          Only dump keys that expire after these many seconds.
  -c,--checkpoint_resume BOOLEAN        Resume dump from previous incomplete run.
  -s,--s3_dump BOOLEAN                  Uploads dumped files to S3 if set.
  --s3_bucket TEXT                      S3 Bucket name.
  --s3_path TEXT                        S3 Final Path.
  -r,--req_id TEXT REQUIRED             Dump ID assigned by requesting service.


```