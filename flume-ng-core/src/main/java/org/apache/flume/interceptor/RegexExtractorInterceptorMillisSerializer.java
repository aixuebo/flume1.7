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

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.google.common.base.Preconditions;

/**
 * Serializer that converts the passed in value into milliseconds using the
 * specified formatting pattern
 * 将传入的日期类型的值,转换成时间戳
 */
public class RegexExtractorInterceptorMillisSerializer implements
    RegexExtractorInterceptorSerializer {

  private DateTimeFormatter formatter;//日期的表达式

  @Override
  public void configure(Context context) {
    String pattern = context.getString("pattern");//日期表达式在配置文件中的key
    Preconditions.checkArgument(!StringUtils.isEmpty(pattern),
        "Must configure with a valid pattern");
    formatter = DateTimeFormat.forPattern(pattern);
  }

  //将日期转换成时间戳
  @Override
  public String serialize(String value) {
    DateTime dateTime = formatter.parseDateTime(value);
    return Long.toString(dateTime.getMillis());
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // NO-OP...
  }
}
