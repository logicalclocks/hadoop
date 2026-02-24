# HDFS Remote Client

Wrapper module that bridges the host's `org.apache.hadoop.fs.FileSystem` API to the shaded Apache HDFS `DistributedFileSystem`. This allows Spark applications running on HopsFS to access external Apache HDFS clusters without classpath conflicts.

## Classes

### `HdfsRemoteFileSystem`

Extends `org.apache.hadoop.fs.FileSystem` (host/HopsFS version). Registered as the handler for `hdfs://` URIs via:

```
spark.hadoop.fs.hdfs.impl=io.hops.hdfs.remote.client.HdfsRemoteFileSystem
```

**`initialize(URI, Configuration)`**:
1. Copies all config entries from host `Configuration` to a shaded `Configuration`
2. Sets the `REMOTE_HADOOP_USER_NAME` env/system property on the shaded UGI if present
3. Calls `UserGroupInformation.setConfiguration()` on the shaded UGI (enables Kerberos if configured)
4. Creates and initializes a shaded `DistributedFileSystem`

All filesystem operations (`open`, `create`, `delete`, `rename`, `listStatus`, `getFileStatus`, `mkdirs`, `append`) delegate to the shaded DFS with type conversion between host and shaded types.

### `HdfsRemoteFSInputStream`

Wraps the shaded `FSDataInputStream` to implement the host's `Seekable` and `PositionedReadable` interfaces. Used by `HdfsRemoteFileSystem.open()`.

## Dependencies

- `hadoop-common` (provided) — host FileSystem API, Configuration, Path, etc.
- `hadoop-client-runtime` (compile) — the shaded Apache HDFS client jar

## Kerberos

Uses the standard system ticket cache. Do `kinit` before running the application. The shaded UGI picks up the TGT automatically — no custom keytab configuration in the wrapper.

## Config Parameter Prefix

All Apache HDFS config keys are renamed with `rahdfs.` prefix to avoid conflicts with HopsFS:

- `hadoop.*` → `rahdfs.*`
- `dfs.*` → `rahdfs.dfs.*`
- `ipc.*` → `rahdfs.ipc.*`
