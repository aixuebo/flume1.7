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
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;

import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An {@link HbaseEventSerializer} which parses columns based on a supplied
 * regular expression and column name list.
 * <p>
 * Note that if the regular expression does not return the correct number of
 * groups for a particular event, or it does not correctly match an event,
 * the event is silently dropped.
 * <p>
 * Row keys for each event consist of a timestamp concatenated with an
 * identifier which enforces uniqueness of keys across flume agents.
 * <p>
 * See static constant variables for configuration options.
 *
 * 如何序列化一个事件,将其序列化成hbase可以接收的rowkey对象
 *
 * 向一个family中添加多个列对应的值,每一个列的值是通过正则表达式去解析出来的。rowkey也是解析出来的
 */
public class RegexHbaseEventSerializer implements HbaseEventSerializer {
  // Config vars
  /** Regular expression used to parse groups from event data. */
  public static final String REGEX_CONFIG = "regex";//正则表达式对应的key
  public static final String REGEX_DEFAULT = "(.*)";//默认的正则表达式

  /** Whether to ignore case when performing regex matches. */
  public static final String IGNORE_CASE_CONFIG = "regexIgnoreCase";//是否忽略大小写
  public static final boolean INGORE_CASE_DEFAULT = false;

  /** Comma separated list of column names to place matching groups in. */
  public static final String COL_NAME_CONFIG = "colNames";//解析列名字集合
  public static final String COLUMN_NAME_DEFAULT = "payload";

  /** Index of the row key in matched regex groups */
  public static final String ROW_KEY_INDEX_CONFIG = "rowKeyIndex";

  /** Placeholder in colNames for row key */
  public static final String ROW_KEY_NAME = "ROW_KEY";//rowkey不能是这个名字

  /** Whether to deposit event headers into corresponding column qualifiers */
  public static final String DEPOSIT_HEADERS_CONFIG = "depositHeaders";
  public static final boolean DEPOSIT_HEADERS_DEFAULT = false;

  /** What charset to use when serializing into HBase's byte arrays */
  public static final String CHARSET_CONFIG = "charset";
  public static final String CHARSET_DEFAULT = "UTF-8";

  /* This is a nonce used in HBase row-keys, such that the same row-key
   * never gets written more than once from within this JVM. */
  protected static final AtomicInteger nonce = new AtomicInteger(0);
  protected static String randomKey = RandomStringUtils.randomAlphanumeric(10);

  protected byte[] cf;//hbase的family
  private byte[] payload;//存储到hbase的具体value
  private List<byte[]> colNames = Lists.newArrayList();//正则表达式需要的列集合
  private Map<String, String> headers;//事件的header集合
  private boolean regexIgnoreCase;//是否忽略大小写
  private boolean depositHeaders;//true表示header也要作为hbase的列添加到hbase中
  private Pattern inputPattern;//具体的正则表达式对象
  private Charset charset;
  private int rowKeyIndex;//rowkey的下标

  @Override
  public void configure(Context context) {
    String regex = context.getString(REGEX_CONFIG, REGEX_DEFAULT);
    regexIgnoreCase = context.getBoolean(IGNORE_CASE_CONFIG,
        INGORE_CASE_DEFAULT);
    depositHeaders = context.getBoolean(DEPOSIT_HEADERS_CONFIG,
        DEPOSIT_HEADERS_DEFAULT);
    inputPattern = Pattern.compile(regex, Pattern.DOTALL
        + (regexIgnoreCase ? Pattern.CASE_INSENSITIVE : 0));
    charset = Charset.forName(context.getString(CHARSET_CONFIG,
        CHARSET_DEFAULT));

    String colNameStr = context.getString(COL_NAME_CONFIG, COLUMN_NAME_DEFAULT);
    String[] columnNames = colNameStr.split(",");
    for (String s : columnNames) {
      colNames.add(s.getBytes(charset));
    }

    //Rowkey is optional, default is -1
    rowKeyIndex = context.getInteger(ROW_KEY_INDEX_CONFIG, -1);
    //if row key is being used, make sure it is specified correct
    if (rowKeyIndex >= 0) {
      if (rowKeyIndex >= columnNames.length) {
        throw new IllegalArgumentException(ROW_KEY_INDEX_CONFIG + " must be " +
            "less than num columns " + columnNames.length);
      }
      if (!ROW_KEY_NAME.equalsIgnoreCase(columnNames[rowKeyIndex])) {
        throw new IllegalArgumentException("Column at " + rowKeyIndex + " must be "
            + ROW_KEY_NAME + " and is " + columnNames[rowKeyIndex]);
      }
    }
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, byte[] columnFamily) {
    this.headers = event.getHeaders();
    this.payload = event.getBody();
    this.cf = columnFamily;
  }

  /**
   * Returns a row-key with the following format:
   * [time in millis]-[random key]-[nonce]
   * 产生一个rowkey
   */
  protected byte[] getRowKey(Calendar cal) {
    /* NOTE: This key generation strategy has the following properties:
     * 
     * 1) Within a single JVM, the same row key will never be duplicated.
     * 2) Amongst any two JVM's operating at different time periods (according
     *    to their respective clocks), the same row key will never be 
     *    duplicated.
     * 3) Amongst any two JVM's operating concurrently (according to their
     *    respective clocks), the odds of duplicating a row-key are non-zero
     *    but infinitesimal. This would require simultaneous collision in (a) 
     *    the timestamp (b) the respective nonce and (c) the random string.
     *    The string is necessary since (a) and (b) could collide if a fleet
     *    of Flume agents are restarted in tandem.
     *    
     *  Row-key uniqueness is important because conflicting row-keys will cause
     *  data loss. */
    String rowKey = String.format("%s-%s-%s", cal.getTimeInMillis(),
        randomKey, nonce.getAndIncrement());
    return rowKey.getBytes(charset);
  }

  protected byte[] getRowKey() {
    return getRowKey(Calendar.getInstance());
  }

  @Override
  public List<Row> getActions() throws FlumeException {
    List<Row> actions = Lists.newArrayList();
    byte[] rowKey;
    Matcher m = inputPattern.matcher(new String(payload, charset));
    if (!m.matches()) {
      return Lists.newArrayList();
    }

    if (m.groupCount() != colNames.size()) {
      return Lists.newArrayList();
    }

    try {
      if (rowKeyIndex < 0) {
        rowKey = getRowKey();
      } else {
        rowKey = m.group(rowKeyIndex + 1).getBytes(Charsets.UTF_8);
      }
      Put put = new Put(rowKey);

      for (int i = 0; i < colNames.size(); i++) {
        if (i != rowKeyIndex) {
          put.add(cf, colNames.get(i), m.group(i + 1).getBytes(Charsets.UTF_8));//该family的某一个列,添加具体的值
        }
      }
      if (depositHeaders) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          put.add(cf, entry.getKey().getBytes(charset), entry.getValue().getBytes(charset));
        }
      }
      actions.add(put);
    } catch (Exception e) {
      throw new FlumeException("Could not get row key!", e);
    }
    return actions;
  }

  @Override
  public List<Increment> getIncrements() {
    return Lists.newArrayList();
  }

  @Override
  public void close() {
  }
}