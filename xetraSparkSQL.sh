#!/bin/bash

SPARK_HOME=/opt/spark
CLASS=xetra.sparksql.App
XETRA_SPARKSQL_HOME=/home/rmunoz/Projects/Acorns
XETRA_SPARKSQL_INPUT=file://${XETRA_SPARKSQL_HOME}/data/*/*.csv
XETRA_SPARKSQL_OUTPUT=${XETRA_SPARKSQL_HOME}/output/
XETRA_SPARKSQL_JAR=${XETRA_SPARKSQL_HOME}/app/target/XetraSparkSQL-0.0.1-SNAPSHOT.jar


sh ${SPARK_HOME}/bin/spark-submit \
 --master local[*] \
 --class ${CLASS} \
 ${XETRA_SPARKSQL_JAR} \
 ${XETRA_SPARKSQL_INPUT} \
 ${XETRA_SPARKSQL_OUTPUT}
