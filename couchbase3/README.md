<!--
Copyright (c) 2015 - 2016 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

# Couchbase (SDK 3.x) Driver for YCSB
This driver is a binding for the YCSB facilities to operate against a Couchbase Server cluster. It uses the official
Couchbase Java SDK (version 3.x) and provides a rich set of configuration options.

## Quickstart

### 1. Start Couchbase Server
You need to start a single node or a cluster to point the client at. Please see [http://couchbase.com](couchbase.com)
for more details and instructions.

### 2. Set up YCSB
You can either download the release zip and run it, or just clone from master.

```
git clone git://github.com/brianfrankcooper/YCSB.git
cd YCSB
mvn -pl site.ycsb:couchbase3-binding -am clean package
```

### 3. Run the Workload
Before you can actually run the workload, you need to "load" the data first.

```
bin/ycsb load couchbase3 -s -P workloads/workloada
```

Then, you can run the workload:

```
bin/ycsb run couchbase3 -s -P workloads/workloada
```

Please see the general instructions in the `doc` folder if you are not sure how it all works. You can apply a property
(as seen in the next section) like this:

```
bin/ycsb run couchbase3 -s -P workloads/workloada -p couchbase.epoll=true
```

## Index Creation
In general, every time SQL++ is used (either implicitly through using `workloade` or through setting `kv=false`) some
kind of index must be present to make it work. Depending on the workload and data size, choosing the right index is
crucial at runtime in order to get the best performance. If in doubt, please ask at the
[forums](http://forums.couchbase.com) or get in touch with our team at Couchbase.

For `workloade` and the default `readallfields=true` we recommend creating the following index, and if using Couchbase
Server 4.5 or later with the "Memory Optimized Index" setting on the bucket.

```
CREATE PRIMARY INDEX ON `bucketname`;
```

For other workloads, different index setups might be even more performant.

## Configuration Options
Since no setup is the same and the goal of YCSB is to deliver realistic benchmarks, here are some setups that you can
tune. Note that if you need more flexibility (let's say a custom transcoder), you still need to extend this driver and
implement the facilities on your own.

You can set the following properties (with the default settings applied):

 - couchbase.host=127.0.0.1: The hostname from one server.
 - couchbase.bucket=ycsb: The bucket name to use.
 - couchbase.scope=_default: The scope to use.
 - couchbase.collection=_default: The collection to use.
 - couchbase.password=: The password of the bucket.
 - couchbase.durability=: Durability level to use.
 - couchbase.persistTo=0: Persistence durability requirement
 - couchbase.replicateTo=0: Replication durability requirement
 - couchbase.upsert=false: Use upsert instead of insert or replace.
 - couchbase.adhoc=false: If set to true, prepared statements are not used.
 - couchbase.maxParallelism=1: The server parallelism for all n1ql queries.
 - couchbase.kvEndpoints=1: The number of KV sockets to open per server.
 - couchbase.sslMode=false: Set to ```true``` to use SSL to connect to the cluster.
 - couchbase.sslNoVerify=true: Set to ```false``` to check the SSL server certificate.
 - couchbase.certificateFile=: Path to file containing certificates to trust.
