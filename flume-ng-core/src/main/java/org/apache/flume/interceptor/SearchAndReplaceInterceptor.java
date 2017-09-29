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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * Interceptor that allows search-and-replace of event body strings using
 * regular expressions. This only works with event bodies that are valid
 * strings. The charset is configurable.
 * 该拦截器的目的是搜索body的正则表达式,将匹配的信息替换成新的信息。
 * <p>
 * Usage:
 * <pre>
 *     比如将INFO替换成Log msg
 *   agent.source-1.interceptors.search-replace.searchPattern = ^INFO:
 *   agent.source-1.interceptors.search-replace.replaceString = Log msg:
 * </pre>
 * <p>
 * Any regular expression search pattern and replacement pattern that can be
 * used with {@link java.util.regex.Matcher#replaceAll(String)} may be used,
 * including backtracking and grouping.
 *
 * 1.将body内容转换成字符串
 * 2.查看该字符串是否有匹配的正则表达式规则
 * 3.将匹配的字符串替换成对应的值
 */
public class SearchAndReplaceInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
      .getLogger(SearchAndReplaceInterceptor.class);

  private final Pattern searchPattern;//准备搜索的匹配的表达式规则
  private final String replaceString;//将匹配的字符串替换成该属性对应的值
  private final Charset charset;//body的编码

  private SearchAndReplaceInterceptor(Pattern searchPattern,
                                      String replaceString,
                                      Charset charset) {
    this.searchPattern = searchPattern;
    this.replaceString = replaceString;
    this.charset = charset;
  }

  @Override
  public void initialize() {
  }

  @Override
  public void close() {
  }

  @Override
  public Event intercept(Event event) {
    String origBody = new String(event.getBody(), charset);//将body内容转换成字符串
    Matcher matcher = searchPattern.matcher(origBody);//查看该字符串是否有匹配的正则表达式规则
    String newBody = matcher.replaceAll(replaceString);//将匹配的字符串替换成该属性对应的值
    event.setBody(newBody.getBytes(charset));//在重新进行字节转换
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  public static class Builder implements Interceptor.Builder {
      //配置文件中的key内容
    private static final String SEARCH_PAT_KEY = "searchPattern";
    private static final String REPLACE_STRING_KEY = "replaceString";
    private static final String CHARSET_KEY = "charset";

    private Pattern searchRegex;
    private String replaceString;
    private Charset charset = Charsets.UTF_8;

    @Override
    public void configure(Context context) {
      String searchPattern = context.getString(SEARCH_PAT_KEY);
      Preconditions.checkArgument(!StringUtils.isEmpty(searchPattern),
          "Must supply a valid search pattern " + SEARCH_PAT_KEY +
          " (may not be empty)");

      replaceString = context.getString(REPLACE_STRING_KEY);
      // Empty replacement String value or if the property itself is not present
      // assign empty string as replacement
      if (replaceString == null) {
        replaceString = "";
      }

      searchRegex = Pattern.compile(searchPattern);

      if (context.containsKey(CHARSET_KEY)) {
        // May throw IllegalArgumentException for unsupported charsets.
        charset = Charset.forName(context.getString(CHARSET_KEY));
      }
    }

    @Override
    public Interceptor build() {
      Preconditions.checkNotNull(searchRegex,
                                 "Regular expression search pattern required");
      Preconditions.checkNotNull(replaceString,
                                 "Replacement string required");
      return new SearchAndReplaceInterceptor(searchRegex, replaceString, charset);
    }
  }
}
