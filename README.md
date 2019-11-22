# Native Memcached dumper

**Note: Still under development. Not ready for use.**

- Build instructions:

From the project root directory, create a new directory called build, and run cmake from there:
```
mkdir build
cd build/
cmake ..
make
```

This will create a bin/ directory under the root project directory. To run:
```
cd ../bin/
./memcache-dumper
```

It currently runs with a dummy configuration that expets a memcached process running in localhost:11211.
It currently only does a key metadata dump.

*More details to follow soon*