# TF-IDF-Hadoop
Realization of TF-IDF algorithm based on MapReduce model
https://en.wikipedia.org/wiki/Tf%E2%80%93idf

This Java classes represent realization of TF-IDF algorithm for corpus of documents.
# Environment for launching HDFS and Hadoop code
Cloudera Virtual machinegives oportunity to lauch one cluster for hadoop experiments. It uses 4 GB RAM.</br>
https://www.cloudera.com/downloads.html

# Description of the process
1) User launch TF-IDF.sh bash script with two arguments. First argument define location of the input folder on local disk and the second argument local disk output folder, where final results will be copied after execution of algorithm.

2) Script counts number of files in input folder. It will be used in the third Job for calculating IDF.

3) Input files is copying to Hadoop Distributed File System into /TF-IDF/input/

4) Script launches **Job_1**</br>
Output of **Job_1** stored in /TF-IDF/j1-output in HDFS</br>
**Mapper:**</br>
Input: <lineNumber, lineOfText></br>
Output: <word#docname, 1></br>                                                                   
**Reducer:**</br>
Input: <word#docname, 1></br>
Output: <word#docname, sum(1+1+..)></br>

5) Script launches **Job_2**</br>
Output of **Job_2** stored in /TF-IDF/j2-output in HDFS</br>
**Mapper:**</br>
Input: <word#docname, n>  <- Input from /TF-IDF/j1-output folder in HDFS</br>
Output: <docname, word=n>></br>                                                                   
**Reducer:**</br>
Input: <docname, word=n></br>
Output: <<word#docname, n/N>></br>
6) Script launches **Job_3** </br>
Output of **Job_3** stored in /TF-IDF/output in HDFS. This is final output.</br>
**Mapper:**</br>
Input: <NumLine, word#docname n/N> <- Input from /TF-IDF/j2-output folder in HDFS</br>
Output: <word, docname=n/N></br>                                                                   
**Reducer:**</br>
Input: <word, docname=n/N></br>
Output: <word#docname, TF-IDF = tf_idf TF = tf IDF = idf></br>
7) Copying /TF-IDF/output folder to local disk in folder, specified by second argument in script launching</br>  
