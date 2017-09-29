/*
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
package org.apache.flume.util;

import java.util.Iterator;
import java.util.List;

/**
 * An implementation of OrderSelector which returns objects in round robin order.
 * Also supports backoff.
 * 实现的一个
 */
public class RoundRobinOrderSelector<T> extends OrderSelector<T> {

  private int nextHead = 0;//下一个选择第几个sink

  public RoundRobinOrderSelector(boolean shouldBackOff) {
    super(shouldBackOff);
  }

  @Override
  public Iterator<T> createIterator() {
    List<Integer> activeIndices = getIndexList();//返回可用的sink集合
    int size = activeIndices.size();//一共多少个sink
    // possible that the size has shrunk so gotta adjust nextHead for that
    if (nextHead >= size) {//说明要从头开始迭代
      nextHead = 0;
    }
    int begin = nextHead++;
    if (nextHead == activeIndices.size()) {
      nextHead = 0;
    }

    int[] indexOrder = new int[size];//最终的顺序序号

    for (int i = 0; i < size; i++) {
      indexOrder[i] = activeIndices.get((begin + i) % size);//以此获取每一个sink对应的序号
    }

    return new SpecificOrderIterator<T>(indexOrder, getObjects());
  }
}
