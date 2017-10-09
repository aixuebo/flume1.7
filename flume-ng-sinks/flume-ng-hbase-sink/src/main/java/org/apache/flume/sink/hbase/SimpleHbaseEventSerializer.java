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

package org.apache.flume.sink.hbase;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.util.LinkedList;
import java.util.List;

/**
 * A simple serializer that returns puts from an event, by writing the event
 * body into it. The headers are discarded. It also updates a row in hbase
 * which acts as an event counter.
 * <p>Takes optional parameters:<p>
 * <tt>rowPrefix:</tt> The prefix to be used. Default: <i>default</i><p>
 * <tt>incrementRow</tt> The row to increment. Default: <i>incRow</i><p>
 * <tt>suffix:</tt> <i>uuid/random/timestamp.</i>Default: <i>uuid</i><p>
 * <p>Mandatory parameters: <p>
 * <tt>cf:</tt>Column family.<p>
 * Components that have no defaults and will not be used if null:
 * <tt>payloadColumn:</tt> Which column to put payload in. If it is null,
 * event data will not be written.<p>
 * <tt>incColumn:</tt> Which column to increment. Null means no column is
 * incremented.
 *
 * 如何对一个事件进行序列化,让hbase接受序列化后的信息
 */
public class SimpleHbaseEventSerializer implements HbaseEventSerializer {
  private String rowPrefix;//rowkey生成前缀
  private byte[] incrementRow;
  private byte[] cf;//family列族
  private byte[] plCol;//一个列族下具体的列
  private byte[] incCol;//计数器对应的列
  private KeyType keyType;//如何生成rowkey的规则
  private byte[] payload;//具体的value

  public SimpleHbaseEventSerializer() {
  }

  @Override
  public void configure(Context context) {
    rowPrefix = context.getString("rowPrefix", "default");
    incrementRow =
        context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
    String suffix = context.getString("suffix", "uuid");

    String payloadColumn = context.getString("payloadColumn", "pCol");
    String incColumn = context.getString("incrementColumn", "iCol");
    if (payloadColumn != null && !payloadColumn.isEmpty()) {
      if (suffix.equals("timestamp")) {
        keyType = KeyType.TS;
      } else if (suffix.equals("random")) {
        keyType = KeyType.RANDOM;
      } else if (suffix.equals("nano")) {
        keyType = KeyType.TSNANO;
      } else {
        keyType = KeyType.UUID;
      }
      plCol = payloadColumn.getBytes(Charsets.UTF_8);
    }
    if (incColumn != null && !incColumn.isEmpty()) {
      incCol = incColumn.getBytes(Charsets.UTF_8);
    }
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  //初始化该事件以及对应的列族family
  @Override
  public void initialize(Event event, byte[] cf) {
    this.payload = event.getBody();
    this.cf = cf;
  }

  //一次添加多行数据,其实就是产生一行数据对象
  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = new LinkedList<Row>();
    if (plCol != null) {
      byte[] rowKey;//创建rowkey
      try {
        if (keyType == KeyType.TS) {
          rowKey = SimpleRowKeyGenerator.getTimestampKey(rowPrefix);
        } else if (keyType == KeyType.RANDOM) {
          rowKey = SimpleRowKeyGenerator.getRandomKey(rowPrefix);
        } else if (keyType == KeyType.TSNANO) {
          rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowPrefix);
        } else {
          rowKey = SimpleRowKeyGenerator.getUUIDKey(rowPrefix);
        }
        Put put = new Put(rowKey);
        put.add(cf, plCol, payload);
        actions.add(put);
      } catch (Exception e) {
        throw new FlumeException("Could not get row key!", e);
      }

    }
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    List<Increment> incs = new LinkedList<Increment>();
    if (incCol != null) {
      Increment inc = new Increment(incrementRow);
      inc.addColumn(cf, incCol, 1);
      incs.add(inc);
    }
    return incs;
  }

  @Override
  public void close() {
  }

  //rowkey的自动产生方式
  public enum KeyType {
    UUID,
    RANDOM,
    TS,
    TSNANO;
  }

}
