#!/bin/sh
cd /home/steve/git/Minibase91/Minibase/src/tests
javac -classpath .:.. TestDriver.java BatchCreate.java
java -classpath .:.. tests.BatchCreate $1 $2 
