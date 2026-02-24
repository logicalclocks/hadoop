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

import org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FSDataInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class HdfsRemoteFSInputStream extends DataInputStream
    implements Seekable, PositionedReadable {

  org.apache.hadoop.remote.shaded.org.apache.hadoop.fs.FSDataInputStream in;

  public HdfsRemoteFSInputStream(FSDataInputStream in) {
    super(in);
    this.in = in;
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return in.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return in.seekToNewSource(targetPos);
  }

  @Override
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return in.read(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    in.readFully(position, buffer, offset, length);
  }

  @Override
  public void readFully(long position, byte[] buffer) throws IOException {
    in.readFully(position, buffer);
  }
}
