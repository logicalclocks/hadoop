#!/bin/bash
#set -e

export JAVA_HOME="${HOME}/usr/bin/jdk_for_hadoop"
export PATH="${HOME}/usr/bin/protocol_buffer_all_versions/protobuf-2.5/src:$PATH"

echo "Applying changes for HDFS remote client..."
./changes-for-hdfs-remote-client.sh

echo "Building only the required modules and their dependencies..."
mvn clean install -DskipTests -Dmaven.javadoc.skip=true \
  -pl hadoop-client-modules/hadoop-client-runtime,hadoop-client-modules/hdfs-remote-client -am

echo "Build complete."
echo "Shaded jar: hadoop-client-modules/hadoop-client-runtime/target/hadoop-client-runtime-3.1.1.jar"
echo "Wrapper jar: hadoop-client-modules/hdfs-remote-client/target/hdfs-remote-client-3.1.1.jar"
