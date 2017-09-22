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
package org.apache.flume.interceptor;

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.List;

import org.apache.flume.Event;
import com.google.common.collect.Lists;

/**
 * Implementation of Interceptor that calls a list of other Interceptors
 * serially.
 * 实现拦截器集合--顺序的调用
 */
public class InterceptorChain implements Interceptor {

  // list of interceptors that will be traversed, in order
  private List<Interceptor> interceptors;//按照顺序调用拦截器

  public InterceptorChain() {
    interceptors = Lists.newLinkedList();
  }

  public void setInterceptors(List<Interceptor> interceptors) {
    this.interceptors = interceptors;
  }

    //拦截一个事件
  @Override
  public Event intercept(Event event) {
    for (Interceptor interceptor : interceptors) {//让每一个拦截器对事件进行处理,返回新的事件进入下一个拦截器
      if (event == null) {
        return null;
      }
      event = interceptor.intercept(event);
    }
    return event;
  }

   //拦截一个事件集合
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Interceptor interceptor : interceptors) {
      if (events.isEmpty()) {
        return events;
      }
      events = interceptor.intercept(events);
      Preconditions.checkNotNull(events,
          "Event list returned null from interceptor %s", interceptor);
    }
    return events;
  }

  //对每一个拦截器进行初始化
  @Override
  public void initialize() {
    Iterator<Interceptor> iter = interceptors.iterator();
    while (iter.hasNext()) {
      Interceptor interceptor = iter.next();
      interceptor.initialize();
    }
  }

  //关闭每一个拦截器
  @Override
  public void close() {
    Iterator<Interceptor> iter = interceptors.iterator();
    while (iter.hasNext()) {
      Interceptor interceptor = iter.next();
      interceptor.close();
    }
  }

}
