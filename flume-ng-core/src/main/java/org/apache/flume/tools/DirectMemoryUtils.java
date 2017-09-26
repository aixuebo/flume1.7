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
package org.apache.flume.tools;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class DirectMemoryUtils {

  private static final Logger LOG = LoggerFactory
      .getLogger(DirectMemoryUtils.class);
  private static final String MAX_DIRECT_MEMORY_PARAM =
      "-XX:MaxDirectMemorySize=";
  private static final long DEFAULT_SIZE = getDefaultDirectMemorySize();
  private static final AtomicInteger allocated = new AtomicInteger(0);//当前已经分配的内存数

   //先分配一定空间
  public static ByteBuffer allocate(int size) {
    Preconditions.checkArgument(size > 0, "Size must be greater than zero");
    long maxDirectMemory = getDirectMemorySize();//获取最大可以使用多少内存
    long allocatedCurrently = allocated.get();
    LOG.info("Direct Memory Allocation: " +
        " Allocation = " + size + //等待分配数量
        ", Allocated = " + allocatedCurrently +//已经分配数量
        ", MaxDirectMemorySize = " + maxDirectMemory + //最多分配数
        ", Remaining = " + Math.max(0,(maxDirectMemory - allocatedCurrently)));//剩余多少
    try {
      ByteBuffer result = ByteBuffer.allocateDirect(size);
      allocated.addAndGet(size);
      return result;
    } catch (OutOfMemoryError error) {
      LOG.error("Error allocating " + size + ", you likely want" +
          " to increase " + MAX_DIRECT_MEMORY_PARAM, error);
      throw error;
    }
  }

  public static void clean(ByteBuffer buffer) throws Exception {
    Preconditions.checkArgument(buffer.isDirect(),
        "buffer isn't direct!");
    Method cleanerMethod = buffer.getClass().getMethod("cleaner");
    cleanerMethod.setAccessible(true);
    Object cleaner = cleanerMethod.invoke(buffer);
    Method cleanMethod = cleaner.getClass().getMethod("clean");
    cleanMethod.setAccessible(true);
    cleanMethod.invoke(cleaner);
    allocated.getAndAdd(-buffer.capacity());//减少一部分空间
    long maxDirectMemory = getDirectMemorySize();
    LOG.info("Direct Memory Deallocation: " +
        ", Allocated = " + allocated.get() +
        ", MaxDirectMemorySize = " + maxDirectMemory +
        ", Remaining = " + Math.max(0, (maxDirectMemory - allocated.get())));

  }

    //返回可以使用多大内存
    //返回配置的-XX:MaxDirectMemorySize=属性对应的数据,返回值的单位是byte
  public static long getDirectMemorySize() {
    RuntimeMXBean RuntimemxBean = ManagementFactory.getRuntimeMXBean();
    List<String> arguments = Lists.reverse(RuntimemxBean.getInputArguments());//获取输入的系统参数集合
    long multiplier = 1; //for the byte case.默认单位是byte
    for (String s : arguments) {
      if (s.contains(MAX_DIRECT_MEMORY_PARAM)) {
        String memSize = s.toLowerCase(Locale.ENGLISH)
            .replace(MAX_DIRECT_MEMORY_PARAM.toLowerCase(Locale.ENGLISH), "").trim();

        //将单位转换成byte
        if (memSize.contains("k")) {
          multiplier = 1024;
        } else if (memSize.contains("m")) {
          multiplier = 1048576;
        } else if (memSize.contains("g")) {
          multiplier = 1073741824;
        }
        memSize = memSize.replaceAll("[^\\d]", "");//非数字转换成"",即仅剩下数字
        long retValue = Long.parseLong(memSize);
        return retValue * multiplier;
      }
    }
    return DEFAULT_SIZE;
  }

  //获取默认大小--返回内存的最大使用量
  private static long getDefaultDirectMemorySize() {
    try {
      Class<?> VM = Class.forName("sun.misc.VM");
      Method maxDirectMemory = VM.getDeclaredMethod("maxDirectMemory", (Class<?>)null);
      Object result = maxDirectMemory.invoke(null, (Object[])null);
      if (result != null && result instanceof Long) {
        return (Long)result;
      }
    } catch (Exception e) {
      LOG.info("Unable to get maxDirectMemory from VM: " +
          e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    // default according to VM.maxDirectMemory()
    return Runtime.getRuntime().maxMemory();
  }
}
