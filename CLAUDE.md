# HDFS Remote Shaded Client

## Purpose
From a Hopsworks cluster (Spark built with HopsFS), users need to read from external Apache HDFS clusters. HopsFS and HDFS share class paths (`org.apache.hadoop.*`), causing conflicts. This shaded client resolves that by relocating all HDFS classes to `org.apache.hadoop.remote.shaded.org.apache.hadoop.*`.

## How It Works at Runtime
- Default paths -> HopsFS (via `fs.defaultFS`)
- `hdfs://external-namenode:8020/...` -> Remote HDFS client (via `fs.hdfs.impl` override)
- `hopsfs:///path` -> Write back to HopsFS

## Comparison Table

| | HopsFS Remote Client (existing) | HDFS Remote Client (new) |
|---|---|---|
| Built in | `/home/salman/code/hops/hops` | `/home/salman/code/hops/apache-hadoop` |
| Based on | new `hopsfs-client-api` module | existing `hadoop-client-runtime` (modified) |
| Shade prefix | `io.hops.hadoop.shaded` | `org.apache.hadoop.remote.shaded` |
| URI scheme | `hopsfs://` | `hdfs://` |
| Config prefix | `hops.*` | `rahdfs.*` |
| Wrapper class | `io.hops.hopsfs.client.HopsFileSystem` | `io.hops.hdfs.remote.client.HdfsRemoteFileSystem` |
| Env var for user | `HOPSFS_USER_NAME` | `REMOTE_HADOOP_USER_NAME` |

## Build

Branch: `release-3.1.1-remote-client` (based on `rel/release-3.1.1`)

```bash
bash build-hdfs-remote-client.sh   # ~2.5 min
./upload-jars <suffix>             # upload to repo with optional suffix
```

Requirements:
- Java 8: `~/usr/bin/jdk_for_hadoop`
- Protoc 2.5.0: `~/usr/bin/protocol_buffer_all_versions/protobuf-2.5/src/protoc`
- Maven: `~/usr/bin/maven/bin/mvn`

Output:
- `hadoop-client-runtime-3.1.1.jar` (~40 MB) - shaded HDFS client
- `hdfs-remote-client-3.1.1.jar` (~48 KB) - wrapper

## Config Keys

### Meta-config (consumed by wrapper, not passed to shaded code)
- `rahdfs.core-site.path` - path to remote cluster's core-site.xml
- `rahdfs.hdfs-site.path` - path to remote cluster's hdfs-site.xml
- `rahdfs.keytab.path` - path to Kerberos keytab file
- `rahdfs.keytab.principal` - Kerberos principal (e.g. `user@REALM`)
- `rahdfs.krb5.realm` - Kerberos realm (replaces krb5.conf)
- `rahdfs.krb5.kdc` - KDC server address:port (replaces krb5.conf)

### Config key mapping (critical!)
The build script uses sed to rename config key constants in Hadoop source:
- `"hadoop.X"` -> `"rahdfs.X"` (hadoop. prefix stripped, rahdfs. added)
- `"dfs.X"` -> `"rahdfs.dfs.X"` (rahdfs. prepended)
- `"ipc.X"` -> `"rahdfs.ipc.X"` (rahdfs. prepended)

Examples:
- `hadoop.security.authentication` -> `rahdfs.security.authentication` (NOT `rahdfs.hadoop.security.authentication`)
- `dfs.replication` -> `rahdfs.dfs.replication`

### Override keys (set via Spark hadoopConfiguration, override XML values)
- `rahdfs.security.token.service.use_ip` - set to `"false"` for Kubernetes (DNS mismatch)
- `rahdfs.dfs.client.read.shortcircuit` - set to `"false"` for remote clients

## PySpark Usage

```python
spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.closeAll()

hc = spark.sparkContext._jsc.hadoopConfiguration()
hc.set("fs.hdfs.impl", "io.hops.hdfs.remote.client.HdfsRemoteFileSystem")
hc.set("rahdfs.core-site.path", "/srv/hops/artifacts/core-site.xml")
hc.set("rahdfs.hdfs-site.path", "/srv/hops/artifacts/hdfs-site.xml")
hc.set("rahdfs.keytab.path", "/srv/hops/artifacts/user.keytab")
hc.set("rahdfs.keytab.principal", "user@REALM")
hc.set("rahdfs.krb5.realm", "REALM")
hc.set("rahdfs.krb5.kdc", "kdc.host:88")
hc.set("rahdfs.security.token.service.use_ip", "false")
hc.set("rahdfs.dfs.client.read.shortcircuit", "false")

# Read from remote HDFS
df = spark.read.text("hdfs://nameservice/path/to/file")

# Write back to HopsFS
df.write.text("hopsfs:///tmp/output")
```

**Important**: XML and keytab files must exist on both Spark driver and executor nodes.

## How initialize() Works

1. Load XML files -> parse properties -> `translateKey()` (hadoop.X->rahdfs.X, dfs.X->rahdfs.dfs.X, ipc.X->rahdfs.ipc.X) -> `translateValue()` (shade class names) -> set in shaded config
2. Copy `rahdfs.*` entries from host config as-is (overrides XML values). Keys NOT stripped -- shaded code expects rahdfs.* prefix
3. Set REMOTE_HADOOP_USER_NAME (non-Kerberos auth)
4. Set krb5 realm/kdc JVM system properties (replaces krb5.conf)
5. `UserGroupInformation.setConfiguration(hdfsConf)` + `loginUserFromKeytab()` if keytab provided
6. Initialize shaded DistributedFileSystem

## Modified Files

### HdfsRemoteFileSystem.java (wrapper)
`hadoop-client-modules/hdfs-remote-client/src/main/java/io/hops/hdfs/remote/client/HdfsRemoteFileSystem.java`

- Extends host's `FileSystem`, delegates to shaded `DistributedFileSystem`
- XML config loading with key translation and class name value translation
- Keytab login via `UserGroupInformation.loginUserFromKeytab()`
- krb5 realm/kdc via JVM system properties

### RemoteException.java (HA failover fix)
`hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ipc/RemoteException.java`

- Added `classNameMatches()` method using `endsWith("." + remote)`
- Fixes HA failover: server sends un-shaded `org.apache.hadoop.ipc.StandbyException` but shaded client has `org.apache.hadoop.remote.shaded.org.apache.hadoop.ipc.StandbyException`
- Without this fix, StandbyException isn't recognized and the client never fails over from standby to active namenode
- Uses `endsWith` instead of hardcoded package prefix because the shade plugin mangles string constants containing `org.apache.hadoop.`

### translateValue() note
The `HADOOP_PKG` and `SHADED_PKG` string constants in `HdfsRemoteFileSystem.java` WILL be mangled by the shade plugin at build time. This is correct because `translateValue()` only processes runtime values from XML files (which are NOT shaded).

## Build Scripts

### `changes-to-build-the-hdfs-remote-client.patch`
Patches 3 source files:
1. **UserGroupInformation.java**: `HADOOP_USER_NAME` constant -> `"REMOTE_HADOOP_USER_NAME"`, made `public`
2. **NativeCodeLoader.java**: Comments out `System.loadLibrary("hadoop")` block (native C++ not shaded)
3. **HdfsConstants.java**: Adds `MARKER.` prefix to `CLIENT_NAMENODE_PROTOCOL_NAME` (prevents shader from mangling wire protocol name)

### `changes-for-hdfs-remote-client.sh`
Main script that applies all changes before Maven build:
1. Applies source code patch
2. Renames config params with `rahdfs.` prefix (sed on CommonConfigurationKeysPublic, SSLFactory, DFSConfigKeys, HdfsClientConfigKeys)
3. Removes `core-default.xml` and `hdfs-default.xml`
4. Modifies `hadoop-client-runtime/pom.xml`:
   - Changes shade prefix to `org.apache.hadoop.remote.shaded`
   - Removes `hadoop-client-api` runtime dependency and artifactSet exclude
   - Removes htrace dependency and excludes (3.1.1 specific)
   - Removes filter excluding `org.apache.hadoop:*` classes
   - Removes `org/apache/hadoop/**` from relocation exclusions
   - Removes `org/apache/htrace/**` from relocation exclusions (3.1.1 specific)
   - Adds MARKER relocation rule (strips `MARKER.` prefix, restoring original protocol name)
5. Updates `hadoop-client-modules/pom.xml`: adds `hdfs-remote-client` module, removes invariant check modules

### `build-hdfs-remote-client.sh`
Sets `JAVA_HOME`, protoc `PATH`, runs changes script, runs targeted Maven build.

### `upload-jars`
Uploads built jars to `repo:/opt/repository/master/hdfs-remote-clients` with optional suffix.

## Bugs Fixed (2026-02-25)

1. **Step 2 was stripping rahdfs. prefix** -- overrides like `rahdfs.security.token.service.use_ip` were set as `security.token.service.use_ip` but shaded code expects the prefix. Fixed: pass keys as-is.
2. **XML class name values not translated** -- `ConfiguredFailoverProxyProvider` couldn't be loaded because shaded code needs the `org.apache.hadoop.remote.shaded.` prefix. Fixed: `translateValue()`.
3. **HA failover broken by shading** -- `RemoteException.unwrapRemoteException(StandbyException.class)` failed because shaded vs un-shaded class names didn't match. Fixed: shade-proof `classNameMatches()` using `endsWith`.
4. **First RemoteException fix attempt failed** -- hardcoded `SHADED_PREFIX = "org.apache.hadoop.remote.shaded."` was double-mangled by shade plugin. Fixed: use `endsWith` with no package name strings.

## Key Design Decisions

1. **Use `hdfs://` scheme** (not a custom scheme) - override via `fs.hdfs.impl`
2. **Config prefix `rahdfs.`** - avoids clashing with HopsFS's `hadoop.` and `dfs.` configs
3. **MARKER trick** - protocol name strings must match remote namenode's expectations; MARKER prevents shader from relocating them, then a relocation rule strips MARKER
4. **Don't upgrade protobuf** - shading handles isolation; upgrading breaks hadoop-thirdparty compatibility
5. **Remove default XML configs** - prevents loading conflicting defaults from the shaded jar
6. **Shade htrace** (3.1.1) - avoids htrace class conflicts with host; not needed in newer Hadoop which removed htrace
7. **Keytab login** - uses `UserGroupInformation.loginUserFromKeytab()` directly, no kinit needed
8. **krb5 via system properties** - `java.security.krb5.realm` + `java.security.krb5.kdc` replace krb5.conf
9. **Shade-proof RemoteException** - `endsWith` comparison avoids shade plugin mangling string constants

## Build Errors and Fixes

### Protoc version mismatch
- **Error**: `protoc version is 'libprotoc 3.20.3', expected version is '2.5.0'`
- **Fix**: Use `protobuf-2.5/src/protoc` in PATH for 3.1.1

### Protobuf API incompatibility (3.4.0-SNAPSHOT only)
- **Error**: `cannot find symbol: class UnusedPrivateParameter` when upgrading protobuf
- **Fix**: Keep protobuf versions as-is. Shading relocates protobuf classes so they don't conflict anyway.

## Test Project: `/home/salman/code/hops/hdfs-remote-client-test/`
- `App.java`: 4 tests: `hdfs` (simple), `hopsfs`, `both`, `kerberos`
- `test.sh`: spark-submit with `--jars` for both runtime and wrapper jars
- Uses JDK 11 for running (Java 8 incompatible with MIT Kerberos 1.21+)

## Kerberos Test Setup: `/home/salman/code/hops/hdfs-remote-client-test/hadoop-3.1.1/`
- `setup-kerberos.sh`: install KDC, create realm EXAMPLE.COM
- `start-hdfs-kerberos.sh`: create principals, keytabs, start kerberized HDFS
- Realm: `EXAMPLE.COM`, Principals: `nn/localhost`, `dn/localhost`, `$USER/localhost`
