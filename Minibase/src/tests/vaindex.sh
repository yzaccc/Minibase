#!/bin/sh
cd /home/steve/git/Minibase91/Minibase/src/tests
javac -classpath .:.. TestDriver.java ColumnIndexing.java
java -classpath .:.. tests.ColumnIndexing $1 $2 $3 $4
