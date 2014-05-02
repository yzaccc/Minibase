#!/bin/sh
cd /home/jinxuanw/Minibase/Minibase/src/tests
javac -classpath .:.. TestDriver.java Query.java
java -classpath .:.. tests.Query $1 $2 $3 $4
