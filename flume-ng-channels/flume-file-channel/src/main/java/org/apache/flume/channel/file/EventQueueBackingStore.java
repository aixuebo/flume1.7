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
package org.apache.flume.channel.file;

import java.io.IOException;

import com.google.common.collect.ImmutableSortedSet;

//用于存储事件的队列
abstract class EventQueueBackingStore {
  protected static final int EMPTY = 0;
  private int queueSize;
  private int queueHead;
  private long logWriteOrderID;
  private final int capacity;//队列容量
  private final String name;//队列名字
  public static final String BACKUP_COMPLETE_FILENAME = "backupComplete";//完成的文件名字
  protected Boolean slowdownBackup = false;

  protected EventQueueBackingStore(int capacity, String name) {
    this.capacity = capacity;
    this.name = name;
  }

  //开始和进行checkpoint操作
  abstract void beginCheckpoint() throws IOException;
  abstract void checkpoint() throws IOException;

  //增加和减少文件对应的数量
  abstract void incrementFileID(int fileID);
  abstract void decrementFileID(int fileID);
  abstract ImmutableSortedSet<Integer> getReferenceCounts();
  abstract long get(int index);
  abstract void put(int index, long value);
  abstract boolean syncRequired();
  abstract void close() throws IOException;

  protected abstract int getVersion();

  int getSize() {
    return queueSize;
  }
  void setSize(int size) {
    queueSize = size;
  }
  int getHead() {
    return queueHead;
  }
  void setHead(int head) {
    queueHead = head;
  }
  int getCapacity() {
    return capacity;
  }

  String getName() {
    return name;
  }
  protected void setLogWriteOrderID(long logWriteOrderID) {
    this.logWriteOrderID = logWriteOrderID;
  }
  long getLogWriteOrderID() {
    return logWriteOrderID;
  }

}
