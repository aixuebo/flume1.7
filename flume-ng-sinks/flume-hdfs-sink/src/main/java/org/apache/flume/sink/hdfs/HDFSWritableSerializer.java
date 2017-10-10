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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.Collections;

//将一个事件序列化成key是long,value是字节数组
//long是系统时间戳,value是事件的body内容
public class HDFSWritableSerializer implements SequenceFileSerializer {

  //将事件转换成字节数组对象
  private BytesWritable makeByteWritable(Event e) {
    BytesWritable bytesObject = new BytesWritable();
    bytesObject.set(e.getBody(), 0, e.getBody().length);
    return bytesObject;
  }

  @Override
  public Class<LongWritable> getKeyClass() {
    return LongWritable.class;
  }

  @Override
  public Class<BytesWritable> getValueClass() {
    return BytesWritable.class;
  }

  @Override
  public Iterable<Record> serialize(Event e) {
    Object key = getKey(e);
    Object value = getValue(e);
    return Collections.singletonList(new Record(key, value));
  }

  //将一个事件转换成key需要的类型,即LongWritable类型
  //获取header对应的时间戳,否则就使用系统当前时间戳
  private Object getKey(Event e) {
    String timestamp = e.getHeaders().get("timestamp");
    long eventStamp;

    if (timestamp == null) {
      eventStamp = System.currentTimeMillis();
    } else {
      eventStamp = Long.valueOf(timestamp);
    }
    return new LongWritable(eventStamp);
  }

  //将一个事件转换成value需要的类型,即BytesWritable类型
  private Object getValue(Event e) {
    return makeByteWritable(e);
  }

  public static class Builder implements SequenceFileSerializer.Builder {

    @Override
    public SequenceFileSerializer build(Context context) {
      return new HDFSWritableSerializer();
    }

  }

}
