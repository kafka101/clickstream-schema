#!/bin/bash

rm -rf confluent
mkdir confluent
cd confluent
git clone https://github.com/confluentinc/common.git
git clone https://github.com/confluentinc/rest-utils.git
git clone https://github.com/confluentinc/schema-registry.git
cd common
mvn clean install
cd ../rest-utils
mvn clean install
cd ../schema-registry
mvn clean install
