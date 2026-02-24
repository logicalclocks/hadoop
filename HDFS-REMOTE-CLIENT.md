# HDFS Remote Client for Hopsworks

A shaded Apache HDFS client that allows Spark applications running on a Hopsworks cluster (with HopsFS) to read from and write to external Apache HDFS clusters. HopsFS and Apache HDFS share the `org.apache.hadoop.*` class path, so this client shades all Apache HDFS classes under `org.apache.hadoop.remote.shaded.*` to avoid conflicts.

## Section 1: Using the HDFS Remote Client

### Prerequisites

- A running Spark cluster on Hopsworks (with HopsFS as the default filesystem)
- Two jar files:
  - `hadoop-client-runtime-3.1.1.jar` — the shaded Apache HDFS client
  - `hdfs-remote-client-3.1.1.jar` — the wrapper that bridges HopsFS's `FileSystem` API to the shaded client

### Adding the Jars to Spark

Pass both jars via `--jars` when submitting your Spark application:

```bash
spark-submit \
  --class com.example.MyApp \
  --jars "hadoop-client-runtime-3.1.1.jar,hdfs-remote-client-3.1.1.jar" \
  my-app.jar
```

### Configuration Parameter Naming Convention

The HDFS remote client runs inside the same JVM as HopsFS. To prevent configuration conflicts, all Apache HDFS configuration parameters are renamed with a `rahdfs.` prefix. The renaming rules are:

| Apache HDFS parameter prefix | HDFS Remote Client parameter prefix |
|---|---|
| `hadoop.*` | `rahdfs.*` |
| `dfs.*` | `rahdfs.dfs.*` |
| `ipc.*` | `rahdfs.ipc.*` |

**Examples:**

| Apache HDFS config key | HDFS Remote Client config key |
|---|---|
| `hadoop.security.authentication` | `rahdfs.security.authentication` |
| `hadoop.rpc.socket.factory.class.default` | `rahdfs.rpc.socket.factory.class.default` |
| `dfs.replication` | `rahdfs.dfs.replication` |
| `dfs.namenode.kerberos.principal` | `rahdfs.dfs.namenode.kerberos.principal` |
| `dfs.client.use.datanode.hostname` | `rahdfs.dfs.client.use.datanode.hostname` |
| `ipc.client.connect.timeout` | `rahdfs.ipc.client.connect.timeout` |

This convention applies to **any** Apache HDFS configuration parameter, including those not listed here. To use any standard HDFS configuration option, apply the prefix transformation above and it will be passed through to the shaded HDFS client.

When using Spark, add the standard `spark.hadoop.` prefix:

```
spark.hadoop.rahdfs.dfs.replication=3
```

### Example: Reading from Kerberized HDFS and HopsFS in the Same Spark Application

Obtain a Kerberos ticket before running the application:

```bash
kinit -kt /path/to/user.keytab user/hostname@REALM.COM
```

The shaded HDFS client uses the standard system ticket cache, so no additional Kerberos configuration is needed in the wrapper — just `kinit` before running.

```java
SparkConf config = new SparkConf();

// Register the HDFS remote client as the handler for hdfs:// URIs
config.set("spark.hadoop.fs.hdfs.impl",
    "io.hops.hdfs.remote.client.HdfsRemoteFileSystem");

// Remote HDFS configuration (all use rahdfs. prefix)
config.set("spark.hadoop.rahdfs.dfs.replication", "3");
config.set("spark.hadoop.rahdfs.security.authentication", "kerberos");
config.set("spark.hadoop.rahdfs.dfs.namenode.kerberos.principal",
    "nn/_HOST@REALM.COM");

SparkSession spark = SparkSession.builder()
        .config(config)
        .appName("My App")
        .getOrCreate();

// Read from HopsFS (default filesystem, works as usual)
Dataset<Row> hopsfsData = spark.read()
    .text("hopsfs://hopsfs-namenode:8020/path/to/data");
hopsfsData.show(false);

// Read from external Kerberized Apache HDFS
Dataset<Row> hdfsData = spark.read()
    .text("hdfs://external-namenode:8020/path/to/data");
hdfsData.show(false);

spark.stop();
```

### How It Works

At runtime, two filesystem implementations coexist in the same JVM:

- **HopsFS** handles `hopsfs://` URIs and the default filesystem — using the standard `org.apache.hadoop.*` classes from the Hopsworks classpath
- **HDFS Remote Client** handles `hdfs://` URIs — using the shaded `org.apache.hadoop.remote.shaded.*` classes bundled in the client runtime jar

The wrapper class `HdfsRemoteFileSystem` extends the host's `org.apache.hadoop.fs.FileSystem` so Spark recognizes it as a filesystem provider. Internally, it creates a shaded `DistributedFileSystem` and translates all calls between the host and shaded type systems.

## Section 2: Building the HDFS Remote Client

### Prerequisites

- JDK 8 (`jdk_for_hadoop`)
- Protocol Buffers compiler (`protoc`) version 2.5.0 for Hadoop 3.1.1
- Maven 3.x
- Apache Hadoop source (branch `rel/release-3.1.1`)

### Build Steps

From the Apache Hadoop source root:

```bash
# 1. Start from a clean checkout
git checkout rel/release-3.1.1

# 2. Apply all changes (source patches, config renaming, POM modifications)
./changes-for-hdfs-remote-client.sh

# 3. Build with Maven (targeted build, ~3 minutes)
export JAVA_HOME=/path/to/jdk8
export PATH=/path/to/protobuf-2.5/src:$PATH

mvn clean install -DskipTests -Dmaven.javadoc.skip=true \
  -pl hadoop-client-modules/hadoop-client-runtime,hadoop-client-modules/hdfs-remote-client -am
```

Or use the provided build script which sets up the environment and runs both steps:

```bash
./build-hdfs-remote-client.sh
```

### Output

| Jar | Location | Size |
|---|---|---|
| `hadoop-client-runtime-3.1.1.jar` | `hadoop-client-modules/hadoop-client-runtime/target/` | ~40 MB |
| `hdfs-remote-client-3.1.1.jar` | `hadoop-client-modules/hdfs-remote-client/target/` | ~48 KB |

### What the Build Does

The `changes-for-hdfs-remote-client.sh` script applies the following changes to the Apache Hadoop source before building:

1. **Source code patches** (`changes-to-build-the-hdfs-remote-client.patch`):
   - `UserGroupInformation.java`: renames `HADOOP_USER_NAME` constant to `REMOTE_HADOOP_USER_NAME` and makes it `public`
   - `NativeCodeLoader.java`: disables native library loading (C++ code cannot be shaded)
   - `HdfsConstants.java`: adds a `MARKER.` prefix to the wire protocol name to prevent the shader from mangling it

2. **Config parameter renaming**: renames all configuration keys with the `rahdfs.` prefix (see table in Section 1)

3. **Removes default XML configs**: deletes `core-default.xml` and `hdfs-default.xml` to prevent loading conflicting defaults

4. **Modifies `hadoop-client-runtime/pom.xml`**:
   - Changes shade prefix from `org.apache.hadoop.shaded` to `org.apache.hadoop.remote.shaded`
   - Includes all Hadoop classes in the shaded jar (removes the filter that previously excluded them)
   - Includes htrace in the shaded jar
   - Adds a MARKER relocation rule that strips the `MARKER.` prefix, restoring the original wire protocol name

5. **Adds the `hdfs-remote-client` wrapper module** with `HdfsRemoteFileSystem` and `HdfsRemoteFSInputStream`

6. **Removes invariant check modules** that would fail after the shading changes
