#!/bin/sh
cd /home/jinxuanw/git/Minibase/Minibase/src/tests
javac -classpath .:.. TestDriver.java phase2test.java
java -classpath .:.. tests.phase2test $1 $2 $3 $4
