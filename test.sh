#!/usr/bin/env bash

JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home

if [ ! -x $JAVA_HOME/bin/java ]; then
  JAVA_HOME=/opt/jdk1.8.0_45
fi;

USER_HOME=$HOME

M2=$USER_HOME/.m2/repository
BRTN=$BARATINE_HOME
SOLR=$USER_HOME/projects/solr-5.1.0

cp $USER_HOME/projects/baratine-github/lucene-plugin/service/target/lucene-plugin-service-*.bar $USER_HOME/projects/baratine-github/lucene-plugin/service/lucene-plugin-service.bar

LCN_BAR=$USER_HOME/projects/baratine-github/lucene-plugin/service/lucene-plugin-service.bar

CP=$M2/org/apache/httpcomponents/httpclient/4.4.1/httpclient-4.4.1.jar
CP=$CP:$M2/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar
CP=$CP:$M2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar
CP=$CP:$M2/commons-codec/commons-codec/1.9/commons-codec-1.9.jar
CP=$CP:$M2/org/codehaus/jackson/jackson-core-asl/1.9.11/jackson-core-asl-1.9.11.jar
CP=$CP:$M2/org/codehaus/jackson/jackson-mapper-asl/1.9.11/jackson-mapper-asl-1.9.11.jar
CP=$CP:$USER_HOME/projects/baratine-github/lucene-plugin-stress/target/classes

PORT=8085

WIKI=/Users/alex/projects/data/wiki

MIXED="-c CLIENTS -n 80000 -pre 100 -host localhost -port $PORT -rate 100 -type TYPE -dir $WIKI -file performance.txt"

READ="-c CLIENTS -n 100000 -pre 1000 -host localhost -port $PORT -rate 2147483647 -type TYPE -dir $WIKI -file performance.txt"

BIG="-c CLIENTS -n 500000 -pre 70000 -host localhost -port $PORT -rate 2147483647 -type TYPE -dir $WIKI -file performance.txt"

runbaratine() {
  $BRTN/bin/baratine stop
  rm -rf /tmp/baratine

  $BRTN/bin/baratine start --deploy $LCN_BAR

  sleep 3

  $JAVA_HOME/bin/java -cp $CP test.PerformanceTest $*

  sleep 1

  $BRTN/bin/baratine stop
}

runsolr() {
  rm -rf $SOLR/server/solr/foo/data/*

  $SOLR/bin/solr stop -port $PORT

  $SOLR/bin/solr start -port $PORT -m 2g

  sleep 3

  $JAVA_HOME/bin/java -Xmx2G -cp $CP test.PerformanceTest $*

  $SOLR/bin/solr stop -port $PORT
}

run_1_8() {

  for i in `seq 1 8`; do
    ARGS=`echo $MIXED | sed "s/CLIENTS/$i/g"`
    ARGS_SOLR=`echo $ARGS | sed 's/TYPE/SOLR/g'`
    ARGS_BAR=`echo $ARGS | sed 's/TYPE/BRPC2/g'`

    echo $ARGS_SOLR;
    runsolr $ARGS_SOLR
    runbaratine $ARGS_BAR
  done;

}

run_1_8;

x1() {
  LOAD=$MIXED
  ARGS=`echo $LOAD | sed 's/TYPE/SOLR/g'`
  runsolr $ARGS
  ARGS=`echo $LOAD | sed 's/TYPE/BRPC2/g'`
  runbaratine $ARGS
  LOAD=$READ
  RGS=`echo $LOAD | sed 's/TYPE/SOLR/g'`
  runsolr $ARGS
  ARGS=`echo $LOAD | sed 's/TYPE/BRPC2/g'`
  runbaratine $ARGS
}
