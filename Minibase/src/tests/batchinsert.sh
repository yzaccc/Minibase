#!/bin/sh
cd /home/steve/git/Minibase91/Minibase/src/tests
javac -classpath .:.. TestDriver.java BatchInsert.java
java -classpath .:.. tests.BatchInsert $1 $2 
