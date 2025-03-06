# What is this?

This is a modified version of the Yahoo! Cloud Serving Benchmark (YCSB) that holds a new kind of workload called \`\`. At the time being, the new workload is supported by two database bindings, `cassandra4` and `aerospike7`. That new workload comes with a custom data model and additional functions required in the database bindings. Because of this, the new workload requires modifcations to some of the YCSB's core classes and hence is not compatible with the standard version of the YCSB. 

Note that it is generally possible to include the new workload into the standard version of the YCSB by making e.g. the `DBWrapper` class configurable, we opted not to do so and  keep the new version in a separate branch.

### Workload Implementation 

The workload is implemented in class `AerospikeWorkload` and resides in the ``site.ycsb.workloads package in the core module. It is an extension to the YSCB standard workload, `CoreWorkload` ``. As such, it supports all parameters the standard workload supports. 

In contrast to the standard workload, the `AerospikeWorkload` adds an additional field to the data schema for which a secondary index is created. By default, the new field is called `sifield0` where `siefield` is configurable via the confi parameter `sifieldnameprefix`.  `sifieldnameprefix` cannot be set to the same value as `fieldnameprefix` (parameter of the `CoreWorkload`). It is the responsibility of the Database binding to set-up a secondary index on the `sifield0` column.

The workload supports five different operations whose relative proportion can all be set by the `<operation>proportion` parameter. 

*   `read executes a single query against the primary key (see YCSB standard workload). It can be set via readproportion` (see YCSB core workload).
*   `insert` adds a new item to the database. Its relative proportion can be set via `insertproportion` (see YCSB core workload)`.`
*   `update` adds a new item to the database. Its relative proportion can be set via update`proportion` (see YCSB core workload)`.`
*   `delete` removes a new item from the database by primary key. Its relative proportion can be set via `deleteproportion` (see YCSB core workload)`.`
*   `query executes a single query against the column with the secondary index (see below).` Its relative ratio can be set via queryproportion 

The other standard operations scan and readmodifywrite are not supported with the new workload. Further, if not set the `<operation>proportion` parameters take a value of 0.05.

**Note** the values of the `<operation>proportion` parameters can only be considered true percent when their values add up to 1.00. If this is not the case their values are put into relation; e.g. if all of them are set to 0.05 all five operations will be invoked 20% of the time.

### Workload Characteristics

The general workings of the workload are as with the standard workload. In addition to that, the `sifield0` is populated with an 64bit integer value which is a hash of the primary key counter. Hence, we can expect that there are very few collissions on that field. This property is by design.

### Bindings

# YCSB

[![Build Status](https://travis-ci.org/brianfrankcooper/YCSB.png?branch=master)](https://travis-ci.org/brianfrankcooper/YCSB)

## Links

*   To get here, use https://ycsb.site
*   [Our project docs](https://github.com/brianfrankcooper/YCSB/wiki)
*   [The original announcement from Yahoo!](https://labs.yahoo.com/news/yahoo-cloud-serving-benchmark/)

## Getting Started

Download the [latest release of YCSB](https://github.com/brianfrankcooper/YCSB/releases/latest):

Set up a database to benchmark. There is a README file under each binding  
directory.

Run YCSB command.

On Linux:

On Windows:

Running the `ycsb` command without any argument will print the usage.

See https://github.com/brianfrankcooper/YCSB/wiki/Running-a-Workload  
for a detailed documentation on how to run a workload.

See https://github.com/brianfrankcooper/YCSB/wiki/Core-Properties for  
the list of available workload properties.

## Building from source

YCSB requires the use of Maven 3; if you use Maven 2, you may see [errors](https://github.com/brianfrankcooper/YCSB/issues/406)  
[such as these](https://github.com/brianfrankcooper/YCSB/issues/406).

To build the full distribution, with all database bindings:

```
mvn clean package
```

To build a single database binding:

```
mvn -pl site.ycsb:mongodb-binding -am clean package
```

```
bin/ycsb.bat load basic -P workloads\workloada
bin/ycsb.bat run basic -P workloads\workloada
```

```
bin/ycsb.sh load basic -P workloads/workloada
bin/ycsb.sh run basic -P workloads/workloada
```

```
curl -O --location https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz
tar xfvz ycsb-0.17.0.tar.gz
cd ycsb-0.17.0
```