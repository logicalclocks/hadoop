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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class HdfsRemoteFileSystem extends FileSystem {

  private static final String HDFS_URI_SCHEME = "hdfs";

  DistributedFileSystem dfs;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    org.apache.hadoop.remote.shaded.org.apache.hadoop.conf.Configuration hdfsConf
        = new org.apache.hadoop.remote.shaded.org.apache.hadoop.conf.Configuration();

    // Copy all config entries from host Configuration to shaded Configuration
    Iterator<Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Entry<String, String> entry = it.next();
      hdfsConf.set(entry.getKey(), entry.getValue());
    }

    // Set user name if available from environment
    String userName = System.getenv("REMOTE_HADOOP_USER_NAME");
    if (userName == null) {
      userName = System.getProperty("REMOTE_HADOOP_USER_NAME");
    }
    if (userName != null) {
      System.setProperty(UserGroupInformation.HADOOP_USER_NAME, userName);
    }

    UserGroupInformation.setConfiguration(hdfsConf);

    dfs = new DistributedFileSystem();
    dfs.initialize(uri, hdfsConf);
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
