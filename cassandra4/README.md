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

* `cassandra.keyspace`
  Keyspace name, must match a keyspace created in advance in the target database instance

  * Default value is `ycsb`

* `cassandra.initDefaultTable`
  * Creates the `usertable` if this is set to `true`, default value is `true`
  * setting it to `false` allows creating a table manually with non default parameters
    
* `cassandra.table.columns`
  * The number of columns of the default `usertable`, default value is `10`
  * must match the `fieldcount` parameter of the YCSB 
   
* `cassandra.useSecureBundle`
  - Use the SecureBundle to create the session, default value is `true`
  - the secure bundle contains the `host` and `port` details to connect to the target instance 

* `hosts` 
  * Cassandra nodes to connect to.
  * optional in case of usage of `cassandra.useSecureBundle`

* `port`
  * CQL port for communicating with Cassandra cluster.
  * Default is `9042`
  * optional in case of usage of `cassandra.useSecureBundle`


* `cassandra.username`
* `cassandra.password`
  * Optional user name and password for authentication. 

* `cassandra.path`
  * optional for SSL/TLS connections 
  * The path to the secure connection bundle (especially required for Datastax AstraDB)

* `cassandra.maxconnections`
  * The maximum umber of concurrent connections.
* `cassandra.connecttimeoutmillis`
* `cassandra.requesttimeoutmillis`
  * The connection and request timeout.

* `cassandra.requestconsistencylevel`
  - The Consistency level of the requests, default value is `QUORUM`



