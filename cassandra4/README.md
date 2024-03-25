<!--
Copyright (c) 2015 YCSB contributors. All rights reserved.

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

# Apache Cassandra 4.x CQL binding

Binding for [Apache Cassandra](https://cassandra.apache.org/_/index.html) and [Datastax AstraDB](https://www.datastax.com/), using the version `4.13.0` of the [Datastax Java driver](https://docs.datastax.com/en/developer/java-driver/4.13/).

The binding is tested against:
- Datastax AstraDB ✔️

## Cassandra Configuration Parameters

- `cassandra.keyspace`
  Keyspace name, a table with this name is automatically created if it does not exist.

  - Default value is `ycsb`

- `cassandra.username`
- `cassandra.password`
  - Optional user name and password for authentication. 

- `cassandra.path`
  - The path to the connection bundle

* `cassandra.maxconnections`
  * The maximum umber of concurrent connections.
* `cassandra.connecttimeoutmillis`
* `cassandra.requesttimeoutmillis`
  * The connection and request timeout.
