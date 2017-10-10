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

//如何对事件进行序列化
public interface SequenceFileSerializer {

  //因为HDFS上存储的是key和value,因此要知道key和value的类型
  Class<?> getKeyClass();

  Class<?> getValueClass();

  /**
   * Format the given event into zero, one or more SequenceFile records
   *
   * @param e
   *         event
   * @return a list of records corresponding to the given event
   * 如何对一个事件进行序列化,有时候一个事件可以返回多条记录
   */
  Iterable<Record> serialize(Event e);

  /**
   * Knows how to construct this output formatter.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public SequenceFileSerializer build(Context context);
  }

  /**
   * A key-value pair making up a record in an HDFS SequenceFile
   * 表示要存储的key和value对应的值,因为key和value的类型可能各种各样,因此返回值都是object类型的
   */
  public static class Record {
    private final Object key;
    private final Object value;

    public Record(Object key, Object value) {
      this.key = key;
      this.value = value;
    }

    public Object getKey() {
      return key;
    }

    public Object getValue() {
      return value;
    }
  }

}
