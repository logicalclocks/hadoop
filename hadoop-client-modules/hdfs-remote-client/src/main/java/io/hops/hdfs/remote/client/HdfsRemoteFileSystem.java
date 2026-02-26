/*
 * Copyright (C) 2024 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.hdfs.remote.client;

import org.apache.hadoop.remote.shaded.org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.remote.shaded.org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Bridge between the host HopsFS FileSystem and a remote Apache HDFS cluster.
 *
 * This wrapper allows a Hopsworks/HopsFS cluster to read from and write to
 * an external Apache HDFS cluster. Since HopsFS and HDFS share the same
 * package namespace (org.apache.hadoop.*), the HDFS client classes are shaded
 * to org.apache.hadoop.remote.shaded.org.apache.hadoop.* to avoid conflicts.
 *
 * Configuration:
 *   The wrapper accepts standard core-site.xml and hdfs-site.xml files from
 *   the remote HDFS cluster. Keys are translated at runtime to match the
 *   rahdfs.* prefix used by the shaded code, and class name values are
 *   translated to use the shaded package prefix.
 *
 *   Meta-config keys (set via Spark's hadoopConfiguration):
 *     rahdfs.core-site.path     - path to the remote cluster's core-site.xml
 *     rahdfs.hdfs-site.path     - path to the remote cluster's hdfs-site.xml
 *     rahdfs.keytab.path        - path to a Kerberos keytab file
 *     rahdfs.keytab.principal   - Kerberos principal (e.g. user@REALM)
 *     rahdfs.krb5.realm         - Kerberos realm (replaces krb5.conf)
 *     rahdfs.krb5.kdc           - KDC server address:port (replaces krb5.conf)
 *
 *   Additional rahdfs.* properties can be set to override XML values.
 *   For non-Kerberos clusters, REMOTE_HADOOP_USER_NAME env var sets the user.
 *
 * Example PySpark usage:
 *   hc = spark.sparkContext._jsc.hadoopConfiguration()
 *   hc.set("fs.hdfs.impl", "io.hops.hdfs.remote.client.HdfsRemoteFileSystem")
 *   hc.set("rahdfs.core-site.path", "/path/to/core-site.xml")
 *   hc.set("rahdfs.hdfs-site.path", "/path/to/hdfs-site.xml")
 *   hc.set("rahdfs.keytab.path", "/path/to/user.keytab")
 *   hc.set("rahdfs.keytab.principal", "user@REALM")
 *   hc.set("rahdfs.krb5.realm", "REALM")
 *   hc.set("rahdfs.krb5.kdc", "kdc.host:88")
 *   df = spark.read.text("hdfs://nameservice/path/to/file")
 */
public class HdfsRemoteFileSystem extends FileSystem {

  private static final String HDFS_URI_SCHEME = "hdfs";
  private static final String CONF_PREFIX = "rahdfs.";

  // Meta-config keys consumed by this wrapper only, not passed to shaded Configuration
  private static final Set<String> META_KEYS = new HashSet<>();
  static {
    META_KEYS.add("core-site.path");
    META_KEYS.add("hdfs-site.path");
    META_KEYS.add("keytab.path");
    META_KEYS.add("keytab.principal");
    META_KEYS.add("krb5.realm");
    META_KEYS.add("krb5.kdc");
  }

  DistributedFileSystem dfs;

  /**
   * Initialize the remote HDFS filesystem.
   *
   * Processing order:
   *   1. Load XML config files (core-site.xml, hdfs-site.xml) — keys are translated
   *      from standard Hadoop names to rahdfs.* prefixed names, and class name values
   *      are translated to use the shaded package prefix.
   *   2. Apply rahdfs.* overrides from host config (e.g. Spark hadoopConfiguration).
   *      These are passed as-is since the shaded code expects the rahdfs.* prefix.
   *      Overrides take precedence over XML values.
   *   3. Set REMOTE_HADOOP_USER_NAME if available (non-Kerberos auth).
   *   4. Set JVM-level Kerberos properties (realm, KDC) to replace krb5.conf.
   *   5. Initialize shaded UGI and perform keytab login if configured.
   *   6. Initialize the shaded DistributedFileSystem.
   */
  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    org.apache.hadoop.remote.shaded.org.apache.hadoop.conf.Configuration hdfsConf
        = new org.apache.hadoop.remote.shaded.org.apache.hadoop.conf.Configuration(false);

    // Step 1: Load XML config files if provided.
    // XML files use standard Hadoop key names (e.g. dfs.nameservices, hadoop.rpc.protection).
    // translateKey() converts them to the rahdfs.* prefix the shaded code expects.
    // translateValue() converts class name values to use the shaded package prefix.
    String coreSitePath = conf.get(CONF_PREFIX + "core-site.path");
    String hdfsSitePath = conf.get(CONF_PREFIX + "hdfs-site.path");
    if (coreSitePath != null) {
      loadXmlConfig(hdfsConf, coreSitePath);
    }
    if (hdfsSitePath != null) {
      loadXmlConfig(hdfsConf, hdfsSitePath);
    }

    // Step 2: Copy rahdfs.* entries from host config as-is (these override XML values).
    // Keys already have the rahdfs.* prefix (e.g. rahdfs.security.token.service.use_ip)
    // which is what the shaded code expects, so they are NOT stripped.
    // Values are translated in case they contain Hadoop class names.
    Iterator<Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Entry<String, String> entry = it.next();
      String key = entry.getKey();
      if (key.startsWith(CONF_PREFIX)) {
        String suffix = key.substring(CONF_PREFIX.length());
        if (!META_KEYS.contains(suffix)) {
          hdfsConf.set(key, translateValue(entry.getValue()));
        }
      }
    }

    // Step 3: Set user name if available from environment (non-Kerberos auth)
    String userName = System.getenv("REMOTE_HADOOP_USER_NAME");
    if (userName == null) {
      userName = System.getProperty("REMOTE_HADOOP_USER_NAME");
    }
    if (userName != null) {
      System.setProperty(UserGroupInformation.HADOOP_USER_NAME, userName);
    }

    // Step 4: Set JVM-level Kerberos realm/KDC properties.
    // These replace the need for a krb5.conf file on each node.
    String krb5Realm = conf.get(CONF_PREFIX + "krb5.realm");
    String krb5Kdc = conf.get(CONF_PREFIX + "krb5.kdc");
    if (krb5Realm != null) {
      System.setProperty("java.security.krb5.realm", krb5Realm);
    }
    if (krb5Kdc != null) {
      System.setProperty("java.security.krb5.kdc", krb5Kdc);
    }

    // Step 5: Initialize shaded UGI and perform keytab login if configured.
    // loginUserFromKeytab() authenticates directly with the KDC using the keytab,
    // eliminating the need for a prior kinit command.
    UserGroupInformation.setConfiguration(hdfsConf);

    String keytabPath = conf.get(CONF_PREFIX + "keytab.path");
    String keytabPrincipal = conf.get(CONF_PREFIX + "keytab.principal");
    if (keytabPath != null && keytabPrincipal != null) {
      UserGroupInformation.loginUserFromKeytab(keytabPrincipal, keytabPath);
    }

    // Step 6: Initialize the shaded DistributedFileSystem
    dfs = new DistributedFileSystem();
    dfs.initialize(uri, hdfsConf);
  }

  /**
   * Load a standard Hadoop XML config file (core-site.xml or hdfs-site.xml)
   * and set properties in the shaded Configuration.
   *
   * The XML files use standard Hadoop key names (e.g. dfs.nameservices,
   * hadoop.security.authentication). These are translated to the rahdfs.*
   * prefixed names that the shaded code expects via translateKey().
   *
   * Values that contain Hadoop class names (e.g. ConfiguredFailoverProxyProvider)
   * are translated to use the shaded package prefix via translateValue().
   */
  private void loadXmlConfig(
      org.apache.hadoop.remote.shaded.org.apache.hadoop.conf.Configuration hdfsConf,
      String xmlPath) throws IOException {
    try {
      Document doc = DocumentBuilderFactory.newInstance()
          .newDocumentBuilder().parse(new File(xmlPath));
      NodeList properties = doc.getElementsByTagName("property");
      for (int i = 0; i < properties.getLength(); i++) {
        Element prop = (Element) properties.item(i);
        NodeList nameNodes = prop.getElementsByTagName("name");
        NodeList valueNodes = prop.getElementsByTagName("value");
        if (nameNodes.getLength() > 0 && valueNodes.getLength() > 0) {
          String name = nameNodes.item(0).getTextContent().trim();
          String value = valueNodes.item(0).getTextContent().trim();
          hdfsConf.set(translateKey(name), translateValue(value));
        }
      }
    } catch (Exception e) {
      throw new IOException("Failed to load XML config: " + xmlPath, e);
    }
  }

  /**
   * Translate standard Hadoop config key names to the rahdfs.* prefixed names
   * used by the shaded code.
   *
   * The build script (changes-for-hdfs-remote-client.sh) uses sed to rename
   * config key constants in the Hadoop source before compilation:
   *   "hadoop.X"  -> "rahdfs.X"     (hadoop. prefix stripped, rahdfs. added)
   *   "dfs.X"     -> "rahdfs.dfs.X" (rahdfs. prepended)
   *   "ipc.X"     -> "rahdfs.ipc.X" (rahdfs. prepended)
   *
   * This method applies the same translation at runtime so standard XML
   * config files can be used without modification.
   */
  private String translateKey(String key) {
    if (key.startsWith("hadoop.")) {
      // hadoop.security.authentication -> rahdfs.security.authentication
      return CONF_PREFIX + key.substring("hadoop.".length());
    } else if (key.startsWith("dfs.")) {
      // dfs.nameservices -> rahdfs.dfs.nameservices
      return CONF_PREFIX + key;
    } else if (key.startsWith("ipc.")) {
      // ipc.client.connect.max.retries -> rahdfs.ipc.client.connect.max.retries
      return CONF_PREFIX + key;
    }
    // Keys with other prefixes (e.g. ha.zookeeper.quorum) pass through unchanged
    return key;
  }

  /**
   * Translate config values that contain Hadoop class names to use the shaded
   * package prefix.
   *
   * The Maven shade plugin relocates all org.apache.hadoop.* classes to
   * org.apache.hadoop.remote.shaded.org.apache.hadoop.* in the runtime JAR.
   * When XML config files reference Hadoop classes by fully-qualified name
   * (e.g. dfs.client.failover.proxy.provider = ConfiguredFailoverProxyProvider),
   * those class names must also be translated so the shaded code can load them.
   *
   * Note: the shade plugin also mangles string constants containing
   * "org.apache.hadoop." in the compiled bytecode, so HADOOP_PKG and SHADED_PKG
   * below will be transformed at build time. This is fine because translateValue()
   * is only called at runtime on values from XML files (which are NOT shaded).
   */
  private static final String HADOOP_PKG = "org.apache.hadoop.";
  private static final String SHADED_PKG = "org.apache.hadoop.remote.shaded.org.apache.hadoop.";

  private String translateValue(String value) {
    if (value != null && value.startsWith(HADOOP_PKG)) {
      return SHADED_PKG + value.substring(HADOOP_PKG.length());
    }
    return value;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FileStatus fileSta = dfs.getFileStatus(toShadedPath(f));
    return toFileStatus(fileSta);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return dfs.mkdirs(toShadedPath(f), toShadedPermission(permission));
  }

  @Override
  public Path getWorkingDirectory() {
    return toPath(dfs.getWorkingDirectory());
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    dfs.setWorkingDirectory(toShadedPath(new_dir));
  }

  @Override
  public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
    org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FileStatus[] status = dfs.listStatus(toShadedPath(f));
    FileStatus[] result = new FileStatus[status.length];
    for (int i = 0; i < status.length; i++) {
      result[i] = toFileStatus(status[i]);
    }
    return result;
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return dfs.delete(toShadedPath(f), recursive);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return dfs.rename(toShadedPath(src), toShadedPath(dst));
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize,
      Progressable progress) throws IOException {
    org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FSDataOutputStream out = dfs.append(toShadedPath(f), bufferSize,
        toShadedProgressable(progress));
    return new FSDataOutputStream(out, null);
  }

  @Override
  public FSDataOutputStream create(Path f,
      FsPermission permission,
      boolean overwrite,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress) throws IOException {
    org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FSDataOutputStream out = dfs.create(toShadedPath(f),
        toShadedPermission(permission),
        overwrite, bufferSize, replication, blockSize, toShadedProgressable(progress));
    return new FSDataOutputStream(out, null);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FSDataInputStream in = dfs.open(toShadedPath(f), bufferSize);
    return new FSDataInputStream(new HdfsRemoteFSInputStream(in));
  }

  @Override
  public URI getUri() {
    return dfs.getUri();
  }

  @Override
  public String getScheme() {
    return HDFS_URI_SCHEME;
  }

  @Override
  public void close() throws IOException {
    if (dfs != null) {
      dfs.close();
    }
    super.close();
  }

  private FileStatus toFileStatus(org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FileStatus fileSta) throws IOException {
    FsPermission permission = new FsPermission(fileSta.getPermission().toShort());
    Path symLink = fileSta.isSymlink() ? toPath(fileSta.getSymlink()) : null;
    Path path = toPath(fileSta.getPath());
    return new FileStatus(fileSta.getLen(), fileSta.isDirectory(), fileSta.getReplication(), fileSta.getBlockSize(),
        fileSta.getModificationTime(),
        fileSta.getAccessTime(), permission, fileSta.getOwner(), fileSta.getGroup(), symLink, path);
  }

  private Path toPath(org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.Path p) {
    return p == null ? null : new Path(p.toUri());
  }

  private org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.Path toShadedPath(Path p) {
    return p == null ? null : new org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.Path(p.toUri());
  }

  private org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.permission.FsPermission toShadedPermission(FsPermission p) {
    return p == null ? null : new org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.permission.FsPermission(p.toShort());
  }

  private org.apache.hadoop.remote.shaded.org.apache.hadoop.util.Progressable toShadedProgressable(Progressable progress) {
    return progress == null ? null : new org.apache.hadoop.remote.shaded.org.apache.hadoop.util.Progressable() {
      @Override
      public void progress() {
        progress.progress();
      }
    };
  }
}
