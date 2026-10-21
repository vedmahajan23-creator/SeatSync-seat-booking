#!/bin/bash
# Runs INSIDE the resourcemanager container.
set -e
cd /tmp

echo "1. Copy the log into HDFS"
hdfs dfs -mkdir -p /input
hdfs dfs -put -f /opt/job/input/booking_log.txt /input/booking_log.txt
hdfs dfs -ls /input

echo "2. Compile and package the MapReduce program"
if command -v javac > /dev/null 2>&1; then
  rm -rf /tmp/wc && mkdir -p /tmp/wc
  javac -classpath "$(hadoop classpath)" -d /tmp/wc /opt/job/WordCount.java
  jar -cf /tmp/wordcount.jar -C /tmp/wc .
  RUN="yarn jar /tmp/wordcount.jar WordCount"
else
  echo "javac not found, using the word count that ships with Hadoop instead"
  EXAMPLES=$(ls $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar | head -1)
  RUN="yarn jar $EXAMPLES wordcount"
fi

echo "3. Run the job on YARN"
hdfs dfs -rm -r -f /output
$RUN /input/booking_log.txt /output

echo "4. Top 20 words"
hdfs dfs -cat /output/part-r-00000 | sort -k2 -nr | head -20
