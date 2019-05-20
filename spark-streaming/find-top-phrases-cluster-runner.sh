#!/bin/bash

usage(){
	echo "Usage: $0 --pathsToTextToAnalyze [path_1] [path_2] [path_n-1] [path_n]"
	exit 1
}

if [[ "$#" -lt 2 ]]; then
    usage
fi

yarn_queue=$2

echo "starting job for yarn_queue : $yarn_queue"

spark-submit1.6 \
--conf "spark.yarn.driver.memoryOverhead=1024" \
--conf "spark.yarn.executor.memoryOverhead=1024" \
--driver-java-options -XX:MaxPermSize=512m \
--master yarn-cluster \
--queue #{yarn_queue} \
--files=log4j.properties \
--driver-memory 4g \
--num-executors 4 \
--executor-memory 4g \
--executor-cores 2 \
--conf "spark.yarn.appMasterEnv.JAVA_HOME=/opt/jre1.8.0" \
--conf "spark.executorEnv.JAVA_HOME=/opt/jre1.8.0" \
--conf "spark.yarn.jar=hdfs://zeus/apps/spark/assembly/spark-assembly-1.5.2-hadoop2.5.0-cdh5.3.1.jar" \
--conf "spark.executor.extraClassPath=/opt/hive_extras/*:/opt/spark_extras/*" \
--class com.tomekl007.FindTopPhrasesJob \
top-words-counter*.jar $@
