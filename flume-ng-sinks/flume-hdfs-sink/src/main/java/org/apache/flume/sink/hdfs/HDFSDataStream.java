/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSDataStream extends AbstractHDFSWriter {

  private static final Logger logger = LoggerFactory.getLogger(HDFSDataStream.class);

  private FSDataOutputStream outStream;//hdfs上输出文件的流
  private String serializerType;//如何序列化信息内容存储到HDFS上
  private Context serializerContext;//返回serializer.下面的配置文件信息
  private EventSerializer serializer;//序列化的流,即对outStream进行了包装
  private boolean useRawLocalFileSystem;

  @Override
  public void configure(Context context) {
    super.configure(context);

    serializerType = context.getString("serializer", "TEXT");
    useRawLocalFileSystem = context.getBoolean("hdfs.useRawLocalFileSystem",
        false);
    serializerContext =
        new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));//返回serializer.下面的配置文件信息
    logger.info("Serializer = " + serializerType + ", UseRawLocalFileSystem = "
        + useRawLocalFileSystem);
  }

  //返回要写的数据文件所在的文件系统
  @VisibleForTesting
  protected FileSystem getDfs(Configuration conf, Path dstPath) throws IOException {
    return dstPath.getFileSystem(conf);
  }

  //在文件系统上存储打开一个文件
  protected void doOpen(Configuration conf, Path dstPath, FileSystem hdfs) throws IOException {
    if (useRawLocalFileSystem) {
      if (hdfs instanceof LocalFileSystem) {
        hdfs = ((LocalFileSystem)hdfs).getRaw();
      } else {
        logger.warn("useRawLocalFileSystem is set to true but file system " +
            "is not of type LocalFileSystem: " + hdfs.getClass().getName());
      }
    }

    //是否追加写操作
    boolean appending = false;
    if (conf.getBoolean("hdfs.append.support", false) == true && hdfs.isFile(dstPath)) {
      outStream = hdfs.append(dstPath);
      appending = true;
    } else {
      outStream = hdfs.create(dstPath);
    }

    serializer = EventSerializerFactory.getInstance(
        serializerType, serializerContext, outStream);//序列化的流,即对outStream进行了包装
    if (appending && !serializer.supportsReopen()) {//说明序列化流不支持追加写数据
      outStream.close();
      serializer = null;
      throw new IOException("serializer (" + serializerType +
          ") does not support append");
    }

    // must call superclass to check for replication issues
    registerCurrentStream(outStream, hdfs, dstPath);//调用注册函数

    if (appending) {
      serializer.afterReopen();
    } else {
      serializer.afterCreate();
    }
  }

  //打开一个文件
  @Override
  public void open(String filePath) throws IOException {
    Configuration conf = new Configuration();
    Path dstPath = new Path(filePath);
    FileSystem hdfs = getDfs(conf, dstPath);
    doOpen(conf, dstPath, hdfs);
  }

  @Override
  public void open(String filePath, CompressionCodec codec,
                   CompressionType cType) throws IOException {
    open(filePath);
  }

  //序列化该事件
  @Override
  public void append(Event e) throws IOException {
    serializer.write(e);
  }

  @Override
  public void sync() throws IOException {
    serializer.flush();
    outStream.flush();
    hflushOrSync(outStream);
  }

  @Override
  public void close() throws IOException {
    serializer.flush();
    serializer.beforeClose();
    outStream.flush();
    hflushOrSync(outStream);
    outStream.close();

    unregisterCurrentStream();
  }

}
