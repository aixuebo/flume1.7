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

package org.apache.flume.interceptor;

//拦截器
public enum InterceptorType {

  TIMESTAMP(org.apache.flume.interceptor.TimestampInterceptor.Builder.class),//追加时间戳信息
  HOST(org.apache.flume.interceptor.HostInterceptor.Builder.class),//追加host信息
  STATIC(org.apache.flume.interceptor.StaticInterceptor.Builder.class),//追加一个静态的,预先配置好的信息到每一个事件中
  REGEX_FILTER(
      org.apache.flume.interceptor.RegexFilteringInterceptor.Builder.class),//属于过滤器拦截器,基于正则表达式进行过滤。即不满足的事件就会被抛弃了
  REGEX_EXTRACTOR(org.apache.flume.interceptor.RegexExtractorInterceptor.Builder.class),//正则表达式抽取数据,将抽取的数据添加到header中
  SEARCH_REPLACE(org.apache.flume.interceptor.SearchAndReplaceInterceptor.Builder.class);//该拦截器的目的是搜索body的正则表达式,将匹配的信息替换成新的信息。

  private final Class<? extends Interceptor.Builder> builderClass;

  private InterceptorType(Class<? extends Interceptor.Builder> builderClass) {
    this.builderClass = builderClass;
  }

  public Class<? extends Interceptor.Builder> getBuilderClass() {
    return builderClass;
  }

}
