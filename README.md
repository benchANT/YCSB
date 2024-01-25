<!--
Copyright (c) 2010 Yahoo! Inc., 2012 - 2016 YCSB contributors.
Copyright (c) 2024 benchANT GmbH. All rights reserved.
All rights reserved.

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

# Preamble

This branch hosts a fork of the YCSB with a new workload. The workload is only realized for the RUN phase
and does not implement a LOAD pahse. For the LAOD phase, it assumes that the databases have been loaded 
using a number of [Telegraf](https://www.influxdata.com/time-series-platform/telegraf/) instances.

The new workload makes a few changes to the default database bindings and therefore cannot be easily
integrated in the main branch of the repository. Most specifically, the workload comes with a set of 
up to 29 queries that need to be mapped to concrete queries by the respective database binding.

Currently supported bindings are:
- AWS Timestream
- Azure Data Explorer
- InfluxDB with API version v3

For running and configuring the new workload, it is best to use `workloads/workload_telegraf` as a template.

The workload has been tested with 
- Maven 3.9.5
- Java JDK 20.0.2; it is beneficial to set `JAVA_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"`

### Build

After adding `mvn` and `java` to your `PATH` you can build the core and the three bindings as follows

* `mvn -pl site.ycsb:core -am package -Dcheckstyle.skip`
* `mvn -pl site.ycsb:influxdb-binding -am  package -Dcheckstyle.skip`
* `mvn -pl site.ycsb:awstimestream-binding -am  package -Dcheckstyle.skip`
* `mvn -pl site.ycsb:azuredataexplorer-binding -am  package -Dcheckstyle.skip`

### Run

The application can be run as follows

* InfluxDB: `./bin/ycsb.sh run influxdb -P workloads/workload_telegraf`
* AWS Timestream `./bin/ycsb.sh run awstimestream -P workloads/workload_telegraf`
* Azure Data Explorer `./bin/ycsb.sh run azuredataexplorer -P workloads/workload_telegraf`

Note that for running AWS Timestream two environment variables need to be set
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`

For Azure Data Explorer, the following environment variables can be used
- `AZURE_TENANT`
- `AZURE_CLIENT_ID`
- `AZURE_SECRET`

For InfluxDB DBaaS, use the following variable:
- `INFLUX_ENTERPRISE_TOKEN`

YCSB
====================================
[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)



Links
-----
* To get here, use https://ycsb.site
* [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
* [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

Getting Started
---------------

1. Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

    ```sh
    curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
    tar xfvz ycsb-0.17.0.tar.gz
    cd ycsb-0.17.0
    ```
    
2. Set up a database to benchmark. There is a README file under each binding 
   directory.

3. Run YCSB command. 

    On Linux:
    ```sh
    bin/ycsb.sh load basic -P workloads/workloada
    bin/ycsb.sh run basic -P workloads/workloada
    ```

    On Windows:
    ```bat
    bin/ycsb.bat load basic -P workloads\workloada
    bin/ycsb.bat run basic -P workloads\workloada
    ```

  Running the `ycsb` command without any argument will print the usage. 
   
  See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload
  for a detailed documentation on how to run a workload.

  See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for 
  the list of available workload properties.


Building from source
--------------------

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors
such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

    mvn clean package

To build a single database binding:

    mvn -pl site.ycsb:mongodb-binding -am clean package
