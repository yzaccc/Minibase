#!/bin/sh
cd /home/steve/git/Minibase91/Minibase/src/tests
javac -classpath .:.. TestDriver.java BatchDelete.java
java -classpath .:.. tests.BatchDelete $1 $2 
