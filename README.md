# memoslap
In this branch I tried to use the [epoll](https://man7.org/linux/man-pages/man7/epoll.7.html) I/O event notification facility instead of [poll](https://man7.org/linux/man-pages/man2/poll.2.html), however performance is worse.
___
**memoslap** is a load benchmark for memcached servers, slightly faster but with many less features than the [**memaslap**](http://docs.libmemcached.org/bin/memaslap.html) benchmark included in libmemcached.

## Features
**memoslap** only supports TCP transport and does not check the correctness of values.
Unlike the **memaslap** benchmark it provides the possibility to close and open new connections during the benchmark based on a maximum number of operations per connection. 

## Usage
```
Usage: memoslap [OPTION...] SERVER PORT
memoslap -- load benchmark for memcached servers

  -c, --clients=NUM          Number of concurrent clients for every thread
                             (default 128)
  -g, --get-share=FRACTION   Share of get operations (default 0.9)
  -k, --keys=NUM             Number of distinct keys to use (default 65536)
  -n, --no-fill              Do not fill the database before starting the
                             benchmark
  -o, --op-per-conn=NUM      Operations to execute for every TCP connection
                             (default 0 = all operations in one connection)
  -r, --runtime=SECS         Run time of the benchmark in seconds (default 10)
  -t, --threads=NUM          Number of threads (default 1)
  -?, --help                 Give this help list
      --usage                Give a short usage message
  -V, --version              Print program version
```

## Compile
```
gcc memoslap.c -o memoslap -lpthread
```