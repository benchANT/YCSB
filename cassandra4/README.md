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

Binding for [Apache Cassandra](http://cassandra.apache.org), using the CQL API
via the [DataStax
driver](https://docs.datastax.com/en/developer/java-driver/4.3/).

## Cassandra Configuration Parameters

- `cassandra.keyspace`
  Keyspace name - must match the keyspace for the table created (see above).
  See http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html for details.

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
