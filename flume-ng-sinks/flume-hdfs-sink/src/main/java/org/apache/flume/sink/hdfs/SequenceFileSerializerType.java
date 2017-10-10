/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.hdfs;

//序列化类型方式,即如何对事件进行序列化
public enum SequenceFileSerializerType {
  Writable(HDFSWritableSerializer.Builder.class),//key是系统时间戳,value是事件的body内容,即就是二进制字节数组
  Text(HDFSTextSerializer.Builder.class),//key是系统时间戳,value是事件的body内容转换成文本的Text
  Other(null);

  private final Class<? extends SequenceFileSerializer.Builder> builderClass;

  SequenceFileSerializerType(Class<? extends SequenceFileSerializer.Builder> builderClass) {
    this.builderClass = builderClass;
  }

  public Class<? extends SequenceFileSerializer.Builder> getBuilderClass() {
    return builderClass;
  }

}

