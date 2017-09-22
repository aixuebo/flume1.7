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
package org.apache.flume.channel;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 复制渠道选择器
 * 多工的选择器
 */
public class MultiplexingChannelSelector extends AbstractChannelSelector {

  //事件中的key存储要选择的channel集合对应的key
  public static final String CONFIG_MULTIPLEX_HEADER_NAME = "header";
  public static final String DEFAULT_MULTIPLEX_HEADER =
      "flume.selector.header";


  public static final String CONFIG_PREFIX_MAPPING = "mapping.";
  public static final String CONFIG_DEFAULT_CHANNEL = "default";//获取默认的channel集合
  public static final String CONFIG_PREFIX_OPTIONAL = "optional";

  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(MultiplexingChannelSelector.class);

  private static final List<Channel> EMPTY_LIST =
      Collections.emptyList();

  private String headerName;

  private Map<String, List<Channel>> channelMapping;//添加到组和组下channel集合映射
  private Map<String, List<Channel>> optionalChannels;
  private List<Channel> defaultChannels;

  //通过headerName的值查找对应的channel集合
  @Override
  public List<Channel> getRequiredChannels(Event event) {
    String headerValue = event.getHeaders().get(headerName);
    if (headerValue == null || headerValue.trim().length() == 0) {
      return defaultChannels;
    }

    List<Channel> channels = channelMapping.get(headerValue);

    //This header value does not point to anything
    //Return default channel(s) here.
    if (channels == null) {
      channels = defaultChannels;
    }

    return channels;
  }

   //通过headerName的值查找对应的channel集合
  @Override
  public List<Channel> getOptionalChannels(Event event) {
    String hdr = event.getHeaders().get(headerName);
    List<Channel> channels = optionalChannels.get(hdr);

    if (channels == null) {
      channels = EMPTY_LIST;
    }
    return channels;
  }

  @Override
  public void configure(Context context) {

    //获取事件中对应的channel选择的key
    this.headerName = context.getString(CONFIG_MULTIPLEX_HEADER_NAME,
        DEFAULT_MULTIPLEX_HEADER);

    Map<String, Channel> channelNameMap = getChannelNameMap();//所有渠道组成的集合

    defaultChannels = getChannelListFromNames(
        context.getString(CONFIG_DEFAULT_CHANNEL), channelNameMap);//获取默认的channel集合

    Map<String, String> mapConfig =
        context.getSubProperties(CONFIG_PREFIX_MAPPING);//获取mapping.后面的key=value信息,key是组name,value是组下的channel集合,用空格拆分

    channelMapping = new HashMap<String, List<Channel>>();

    for (String headerValue : mapConfig.keySet()) {
      List<Channel> configuredChannels = getChannelListFromNames(
          mapConfig.get(headerValue),
          channelNameMap);

      //This should not go to default channel(s)
      //because this seems to be a bad way to configure.
      if (configuredChannels.size() == 0) {
        throw new FlumeException("No channel configured for when "
            + "header value is: " + headerValue);
      }

      if (channelMapping.put(headerValue, configuredChannels) != null) {
        throw new FlumeException("Selector channel configured twice");
      }
    }
    //If no mapping is configured, it is ok.
    //All events will go to the default channel(s).
    Map<String, String> optionalChannelsMapping =
        context.getSubProperties(CONFIG_PREFIX_OPTIONAL + ".");//解析optional.

    optionalChannels = new HashMap<String, List<Channel>>();
    for (String hdr : optionalChannelsMapping.keySet()) {
      List<Channel> confChannels = getChannelListFromNames(
              optionalChannelsMapping.get(hdr), channelNameMap);
      if (confChannels.isEmpty()) {
        confChannels = EMPTY_LIST;
      }
      //Remove channels from optional channels, which are already
      //configured to be required channels.

      List<Channel> reqdChannels = channelMapping.get(hdr);
      //Check if there are required channels, else defaults to default channels
      if (reqdChannels == null || reqdChannels.isEmpty()) {
        reqdChannels = defaultChannels;
      }
      for (Channel c : reqdChannels) {
        if (confChannels.contains(c)) {//取消必须的channel
          confChannels.remove(c);
        }
      }

      if (optionalChannels.put(hdr, confChannels) != null) {
        throw new FlumeException("Selector channel configured twice");
      }
    }

  }

}
