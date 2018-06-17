#!/bin/bash

NUM=$(ls ${1} -1|wc -l)

# Puts file from Local FS to HDFS
PUTTER="hadoop fs -put ${1}/* /TF-IDF/input/"
# Get files from HDFS /TF-IDF/output/ folder and copy files to Local FS. Used after execution of all jobs.
GETTER="hadoop fs -get /TF-IDF/output ${2}"

#Location of .jar files with jobs
JAR_1="/home/cloudera/TFIDF/Job1_TF.jar"
JAR_2="/home/cloudera/TFIDF/Job2_TF_w.jar"
JAR_3="/home/cloudera/TFIDF/Job3_TFIDF.jar"

#Location of class with Job runner tool
JAR_1_class="tfidf.Job1_TF"
JAR_2_class="tfidf.Job2_TF_w"
JAR_3_class="tfidf.Job3_TFIDF"

# String with job execution commands
JOB_1_CMD="hadoop jar ${JAR_1} ${JAR_1_class}" 
JOB_2_CMD="hadoop jar ${JAR_2} ${JAR_2_class}" 
JOB_3_CMD="hadoop jar ${JAR_3} ${JAR_3_class} -Dreducer.numF=${NUM}" 

# Strings to remove inputs
INPUT_RMV_CMD="hadoop fs -rm -r /TF-IDF/input/*"
JOB_1_RMV_CMD="hadoop fs -rm -r /TF-IDF/j1-output"
JOB_2_RMV_CMD="hadoop fs -rm -r /TF-IDF/j2-output"

# EXECUTION PHASE

$PUTTER

echo ${JOB_1_CMD}
${JOB_1_CMD}

${INPUT_RMV_CMD}

echo ${JOB_2_CMD}
${JOB_2_CMD}

${JOB_1_RMV_CMD}

echo ${JOB_3_CMD}	
${JOB_3_CMD}	

${JOB_2_RMV_CMD}

$GETTER



