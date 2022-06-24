---
title: "Quick Start"
weight: 1
type: docs
aliases:
- /try-flink-ml/quick-start.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Quick Start

This document provides a quick introduction to using Flink ML. Readers of this
document will be guided to submit a simple Flink job that trains a Machine
Learning Model and use it to provide prediction service.

## Prerequisite

Please make sure you have installed Flink 1.15.0 in your local environment and
configured `$FLINK_HOME`. For instructions of setting up Flink environment,
please refer to [Flink's
document](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/try-flink/local_installation/).

## Download & Compile Flink ML

<!-- TODO: Modify this section after Flink ML's binary distribution is released -->

In order to follow guidelines in this document, you need to have the latest
binary distribution of Flink ML. You can acquire the distribution by building
Flink ML's source code locally with the following command.

```bash
cd ${path_to_flink_ml}
mvn clean package -DskipTests
```

Then you will find Flink ML's binary distribution in
`./flink-ml-dist/target/flink-ml-*-bin/flink-ml*/`. Copy the generated jars to
your Flink environment to finish the setup.

```shell
cd ./flink-ml-dist/target/flink-ml-*-bin/flink-ml*/
cp ./lib/*.jar $FLINK_HOME/lib/
```

## Starting Flink cluster and submitting Flink ML example job

Under `$FLINK_HOME` directory, you may follow these instructions to start a
Flink cluster and submit Flink ML examples to it.

```
./bin/start-cluster.sh
./bin/flink run -c org.apache.flink.ml.examples.clustering.KMeansExample ./lib/flink-ml-examples*.jar
```

The command above would submit and execute Flink ML's `KMeansExample` job. There
are also example jobs for other Flink ML algorithms, and you can find them in
`flink-ml-examples` module.

A sample output in your terminal is as follows.

```
Features: [9.0, 0.0]    Cluster ID: 1
Features: [0.3, 0.0]    Cluster ID: 0
Features: [0.0, 0.3]    Cluster ID: 0
Features: [9.6, 0.0]    Cluster ID: 1
Features: [0.0, 0.0]    Cluster ID: 0
Features: [9.0, 0.6]    Cluster ID: 1

```

You have successfully ran a Flink ML job. Now please feel free to explore other
sections of this document.

