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

import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

/**
 * Interface for an event serializer which serializes the headers and body
 * of an event to write them to hbase. This is configurable, so any config
 * params required should be taken through this. Only the column family is
 * passed in. The columns should exist in the table and column family
 * specified in the configuration for the HbaseSink.
 * 如何序列化一个事件,将其序列化成hbase可以接收的rowkey对象
 */
public interface HbaseEventSerializer extends Configurable, ConfigurableComponent {
  /**
   * Initialize the event serializer.
   * 初始化操作
   * @param event Event to be written to HBase 要写入到hbase的事件
   * @param columnFamily Column family to write to 要写入到那个family里面
   */
  public void initialize(Event event, byte[] columnFamily);

  /**
   * Get the actions that should be written out to hbase as a result of this
   * event. This list is written to hbase using the HBase batch API.
   * @return List of {@link org.apache.hadoop.hbase.client.Row} which
   * are written as such to HBase.
   *
   * 0.92 increments do not implement Row, so this is not generic.
   * 一个事件可以写成多行数据
   */
  public List<Row> getActions();

  public List<Increment> getIncrements();

  /*
   * Clean up any state. This will be called when the sink is being stopped.
   */
  public void close();
}
