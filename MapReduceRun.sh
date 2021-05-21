#!/bin/bash

/opt/hadoop-2.10.1/bin/hdfs namenode -format
/opt/hadoop-2.10.1/sbin/start-dfs.sh
/opt/hadoop-2.10.1/bin/hdfs dfs -mkdir /user
/opt/hadoop-2.10.1/bin/hdfs dfs -mkdir /user/root
 
if $(/opt/hadoop-2.10.1/bin/hdfs dfs -test -d input)
then /opt/hadoop-2.10.1/bin/hdfs dfs -rm -r input
fi
 
if $(/opt/hadoop-2.10.1/bin/hdfs dfs -test -d output)
then /opt/hadoop-2.10.1/bin/hdfs dfs -rm -r output
fi
 
/opt/hadoop-2.10.1/bin/hdfs dfs -put input/ input
 
/opt/hadoop-2.10.1/bin/yarn jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output 60000