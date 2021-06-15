# High Level Architecture

The “native dumper” is used as part of the cache warming workflow to dump the data of a target EVCache cluster into chunked files.
These chunked files are then later used by the Populator to warm-up a destination cluster with the same data. (Note that the "Populator" is not a part of this repo)

The dumping process is broken down into 2 stages:
1. Key dump (or “metadump” in memcached lingo)
2. Data dump (or dumping the values)

It's implemented as a simple multi-threaded task scheduler where each stage listed above is broken down into an independent task.

### System breakdown:
- There exists a task queue which has a queue of tasks waiting to be executed.
- There are “N” task threads that each execute one task at a time.
- A task scheduler dequeues a task from the task queue and assigns it to a free task thread.
- There are “M” fixed size buffers which are allocated on startup and used throughout the lifetime of the process to handle all the data.
- The native dumper tries to stick to a memory limit; which is => buffersize x (num_threads x 2)
- Each task thread has 2 dedicated fixed size buffers. (i.e. M = N * 2)
- The dumper exits once the last task is completed.

### Dumping process:
1. On startup, one task gets enqueued by default which is the meta-dump task which does a key dump to disk.
2. They key dump contains only key metadata and is chunked into key files. This task does the entire first stage.
3. For every key file produced, the meta-dump task enqueues a data-dump task.
4. Each data-dump task looks into the key file assigned to it and requests the values from memcached for all the keys it sees.
5. It then dumps the data for every key into one or more data files. A checksum is calculated for each data file as it’s written and the final file has it as part of its file name. Checksums are used by Cache Populators to validate the file integrity.
6. Once we process all the key files and have dumped all the data files, the dumper finally outputs a file named “DONE” to indicate that it has completed successfully.
7. Optionally, if `is_s3_dump` is selected, the data files will be uploaded to S3 and a SQS message is sent to `sqs_queue_name` for each uploaded file.

The native dumper can fit into an EBS architecture or can fit into using a SQS/S3 architecture. There is no tight dependency on either of the components.
