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

//用什么方式存储序列化的数据
public class HDFSWriterFactory {
  static final String SequenceFileType = "SequenceFile";//对序列化的内容进行Sequence处理,即文件的格式是Sequence格式的
  static final String DataStreamType = "DataStream";//直接存储序列化的原始内容,即文件的格式是Text格式的,只是存储的数据是二进制数据
  static final String CompStreamType = "CompressedStream";//对序列化后的内容进行压缩处理,即文件的格式是Text格式的,只是存储的数据是二进制压缩数据

  public HDFSWriterFactory() {

  }

  public HDFSWriter getWriter(String fileType) throws IOException {
    if (fileType.equalsIgnoreCase(SequenceFileType)) {
      return new HDFSSequenceFile();
    } else if (fileType.equalsIgnoreCase(DataStreamType)) {
      return new HDFSDataStream();
    } else if (fileType.equalsIgnoreCase(CompStreamType)) {
      return new HDFSCompressedDataStream();
    } else {
      throw new IOException("File type " + fileType + " not supported");
    }
  }
}
