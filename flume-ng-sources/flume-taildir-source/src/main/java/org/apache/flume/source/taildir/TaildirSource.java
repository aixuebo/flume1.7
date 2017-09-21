/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

/**
 * 算是读取程序的入口类,通过该类,串联了整个包下的类
 */
public class TaildirSource extends AbstractSource implements
    PollableSource, Configurable {

  private static final Logger logger = LoggerFactory.getLogger(TaildirSource.class);

  private Map<String, String> filePaths;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;//读取json文件,该文件存储每一个file文件最后读取的position位置
  private boolean skipToEnd;//表示从文件的最后开始读取,还是从文件的头开始读取,true表示从文件最后开始读取
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;//可靠的reader去读取数据
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;//说明文件已经长时间没有更新内容了,时间阀值
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();//所有存在的文件集合
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();//说明文件已经长时间没有更新内容了,并且文件尚未被关闭,说明该文件闲置很久了的集合
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;

  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .headerTable(headerTable)
          .positionFilePath(positionFilePath)
          .skipToEnd(skipToEnd)
          .addByteOffset(byteOffsetHeader)
          .cachePatternMatching(cachePatternMatching)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }

    //第一个定时器
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
        idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    //第二个定时器
    positionWriter = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
        writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter};//停止定时服务
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
        positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

    //设置初始化值
  @Override
  public synchronized void configure(Context context) {
      /**
       maming.sources.h5log.filegroups=h5f apf ###定义两个分组
       maming.sources.h5log.filegroups.apf=/server/h5_mark/tomcat/logs/mark.log
       maming.sources.h5log.filegroups.h5f=/server/h5_mark/tomcat/logs/h5Mark.log
       */
    String fileGroups = context.getString(FILE_GROUPS);//获取filegroups对应的值
    Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

      //返回结果是apf=/server/h5_mark/tomcat/logs/mark.log   h5f=/server/h5_mark/tomcat/logs/h5Mark.log
    filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX),
                             fileGroups.split("\\s+")); //获取filegroups.对应的值,并且按照空格拆分filegroups
    Preconditions.checkState(!filePaths.isEmpty(),
        "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

    String homePath = System.getProperty("user.home").replace('\\', '/');
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
    Path positionFile = Paths.get(positionFilePath);
    try {
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }
    headerTable = getTable(context, HEADERS_PREFIX);
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
        DEFAULT_CACHE_PATTERN_MATCHING);

    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

    //从map集合中,获取key[]属性集合对应的值
  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

    //每次都由PollableSourceRunner类调用,去读取日志
  @Override
  public Status process() {
    Status status = Status.READY;
    try {
      existingInodes.clear();
      existingInodes.addAll(reader.updateTailFiles());//获取所有的文件集合
      for (long inode : existingInodes) {
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {//说明有内容,则去读取内容处理
          tailFileProcess(tf, true);
        }
      }
      closeTailFiles();
      try {
        TimeUnit.MILLISECONDS.sleep(retryInterval);
      } catch (InterruptedException e) {
        logger.info("Interrupted while sleeping");
      }
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

    //不断读取一个文件,直到读取文件没有数据未知返回
  private void tailFileProcess(TailFile tf, boolean backoffWithoutNL)
      throws IOException, InterruptedException {
    while (true) {
      reader.setCurrentFile(tf);
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        break;
      }
      sourceCounter.addToEventReceivedCount(events.size());//表示source接收到了多少个事件
      sourceCounter.incrementAppendBatchReceivedCount();//表示source接收到了多少次
      try {
        getChannelProcessor().processEventBatch(events);//发给channel去处理
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
            "The source will try again after " + retryInterval + " ms");
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());//表示成功发送给channel多少个事件
      sourceCounter.incrementAppendBatchAcceptedCount();//表示成功发送给channel多少次
      if (events.size() < batchSize) {
        break;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   * 定期执行,发现哪些文件已经闲置很久了,将其添加到集合中
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        for (TailFile tf : reader.getTailFiles().values()) {//正在读取的文件集合
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {//说明文件已经长时间没有更新内容了,并且文件尚未被关闭
            idleInodes.add(tf.getInode());//说明文件已经长时间没有更新内容了,并且文件尚未被关闭,说明该文件闲置很久了的集合
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   * 写入每一个文件的最后读取位置到文件中
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }

  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      writer = new FileWriter(file);
      if (!existingInodes.isEmpty()) {
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

    //获取活跃的读取的文件
  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }
}
