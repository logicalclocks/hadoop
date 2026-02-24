#!/usr/bin/env bash
#
# Applies all changes needed to build the HDFS remote shaded client.
# Run from the apache-hadoop root directory before the Maven build.
#
set -e

# 1. Apply code patches (UserGroupInformation, NativeCodeLoader, HdfsConstants)
echo "Applying source code patches..."
git apply changes-to-build-the-hdfs-remote-client.patch

# 2. Rename config params (rahdfs. prefix to avoid clashing with HopsFS config)
echo "Renaming config params with rahdfs. prefix..."
sed -i 's/"hadoop\./"rahdfs\./g' \
  hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/CommonConfigurationKeysPublic.java
sed -i 's/"ipc\./"rahdfs.ipc\./g' \
  hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/CommonConfigurationKeysPublic.java
sed -i 's/"hadoop./"rahdfs\./g' \
  hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/security/ssl/SSLFactory.java
sed -i 's/"dfs\./"rahdfs.dfs\./g' \
  hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/DFSConfigKeys.java
sed -i 's/"dfs\./"rahdfs.dfs\./g' \
  hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsClientConfigKeys.java

# 3. Remove default XML configs (avoid loading conflicting defaults)
echo "Removing default XML configs..."
rm -f hadoop-common-project/hadoop-common/src/main/resources/core-default.xml
rm -f hadoop-hdfs-project/hadoop-hdfs/src/main/resources/hdfs-default.xml

# 4. Protobuf version is kept as-is (2.5.0).
#    Shading relocates all protobuf classes so they won't conflict with HopsFS's protobuf 3.20.3.

# 5. Modify hadoop-client-runtime/pom.xml for full shading
echo "Modifying hadoop-client-runtime shading config..."
RUNTIME_POM=hadoop-client-modules/hadoop-client-runtime/pom.xml

# 5a. Change shaded dependency prefix
sed -i 's|<shaded.dependency.prefix>org.apache.hadoop.shaded</shaded.dependency.prefix>|<shaded.dependency.prefix>org.apache.hadoop.remote.shaded</shaded.dependency.prefix>|' \
  "$RUNTIME_POM"

# 5b. Remove hadoop-client-api runtime dependency (everything is now in this jar)
sed -i '/<!-- At runtime anyone using us must have the api present -->/,/<\/dependency>/d' \
  "$RUNTIME_POM"

# 5c. Remove htrace runtime dependency (will be shaded into the jar)
sed -i '/<dependency>/{N;N;N;N;/htrace-core4/d}' "$RUNTIME_POM"

# 5d. Remove hadoop-client-api from artifactSet excludes
sed -i '/<!-- We need a filter that matches just those things that aer included in the api jar -->/d' "$RUNTIME_POM"
sed -i '/<exclude>org.apache.hadoop:hadoop-client-api<\/exclude>/d' "$RUNTIME_POM"

# 5e. Remove htrace from artifactSet excludes (we want it shaded)
sed -i '/<!-- Leave HTrace as an unshaded dependency on purpose/d' "$RUNTIME_POM"
sed -i '/<exclude>org.apache.htrace:htrace-core4<\/exclude>/d' "$RUNTIME_POM"

# 5f. Remove the filter that excludes org.apache.hadoop:* classes
#     (we now want Hadoop classes included in the shaded jar)
sed -i '/<!-- We need a filter that matches just those things that are included in the api jar -->/,/<\/filter>/d' \
  "$RUNTIME_POM"

# 5g. Remove org/apache/hadoop/* exclusions from relocation rules
#     (we now want Hadoop classes relocated under the shade prefix)
sed -i '/<exclude>org\/apache\/hadoop\/\*<\/exclude>/d' "$RUNTIME_POM"
sed -i '/<exclude>org\/apache\/hadoop\/\*\*\/\*<\/exclude>/d' "$RUNTIME_POM"

# 5h. Remove htrace from relocation exclusions (we want it relocated)
sed -i '/<exclude>org\/apache\/htrace\/\*<\/exclude>/d' "$RUNTIME_POM"
sed -i '/<exclude>org\/apache\/htrace\/\*\*\/\*<\/exclude>/d' "$RUNTIME_POM"

# 5i. Add MARKER relocation rule (undoes the MARKER prefix added to protocol names)
sed -i '/<\/relocations>/i\                    <relocation>\
                      <pattern>MARKER.org.apache.hadoop.hdfs.protocol/</pattern>\
                      <shadedPattern>org.apache.hadoop.hdfs.protocol.</shadedPattern>\
                    </relocation>' \
  "$RUNTIME_POM"

# 6. Update hadoop-client-modules/pom.xml
echo "Updating hadoop-client-modules/pom.xml..."
MODULES_POM=hadoop-client-modules/pom.xml

# 6a. Add hdfs-remote-client module
sed -i '/<module>hadoop-client-integration-tests<\/module>/a\    <module>hdfs-remote-client</module>' "$MODULES_POM"

# 6b. Remove invariant check modules (they verify original shading rules which no longer apply)
sed -i '/<!-- Checks invariants above -->/d' "$MODULES_POM"
sed -i '/<module>hadoop-client-check-invariants<\/module>/d' "$MODULES_POM"
sed -i '/<module>hadoop-client-check-test-invariants<\/module>/d' "$MODULES_POM"

echo "All changes applied successfully."
