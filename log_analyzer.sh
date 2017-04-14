#!/bin/bash 
JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
CLASSPATH="/home/akella/Desktop/Assignment" . 
$inputDir1='/home/akella/Desktop/Assignment/iliad'
$inputDir2='/home/akella/Desktop/Assignment/odyssey'
java  -Djava.library.path=./lib/path/Lib -classpath .:lib/path/Lib/mylib.jar Mainclass "$questionNum" "$inputDir1" "$inputDir2" 
exit 0
