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
package org.apache.flume.sink;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.PathManager;
import org.apache.flume.formatter.output.PathManagerFactory;
import org.apache.flume.instrumentation.SinkCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;

//滚动在一个目录下,每隔一定时间周期,产生一个新的文件用于存储接收到的数据
public class RollingFileSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(RollingFileSink.class);
  private static final long defaultRollInterval = 30;
  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;//批处理,一次事务要写入多少条事件

  private File directory;//存储数据的目录
  private long rollInterval;//滚动的时间间隔
  private OutputStream outputStream;//当前正在写入文件的输出流
  private ScheduledExecutorService rollService;//滚动的线程

  private String serializerType;//文件存储到文件中的数据格式
  private Context serializerContext;//获取sink.serializer.下的配置信息
  private EventSerializer serializer;//序列化后的数据写出到哪个流中

  private SinkCounter sinkCounter;//计数器

  private PathManager pathController;//如何切换一个文件
  private volatile boolean shouldRotate;//true表示该滚动了

  public RollingFileSink() {
    shouldRotate = false;
  }

  @Override
  public void configure(Context context) {

    String pathManagerType = context.getString("sink.pathManager", "DEFAULT");
    String directory = context.getString("sink.directory");//存储数据的目录
    String rollInterval = context.getString("sink.rollInterval");//滚动文件的时间间隔

    serializerType = context.getString("sink.serializer", "TEXT");//文件存储到文件中的数据格式,默认是文本格式
    serializerContext =
        new Context(context.getSubProperties("sink." +
            EventSerializer.CTX_PREFIX));//获取sink.serializer.下的配置信息

    Context pathManagerContext =
              new Context(context.getSubProperties("sink." +
                      PathManager.CTX_PREFIX));//获取sink.pathManager.下的配置信息
    pathController = PathManagerFactory.getInstance(pathManagerType, pathManagerContext);

    Preconditions.checkArgument(directory != null, "Directory may not be null");
    Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

    if (rollInterval == null) {
      this.rollInterval = defaultRollInterval;
    } else {
      this.rollInterval = Long.parseLong(rollInterval);
    }

    batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

    this.directory = new File(directory);

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);
    sinkCounter.start();
    super.start();

    pathController.setBaseDirectory(directory);//设置切换文件的基础目录
    if (rollInterval > 0) {//说明文件要定时滚动起来

      rollService = Executors.newScheduledThreadPool(
          1,
          new ThreadFactoryBuilder().setNameFormat(
              "rollingFileSink-roller-" +
          Thread.currentThread().getId() + "-%d").build());

      /*
       * Every N seconds, mark that it's time to rotate. We purposefully do NOT
       * touch anything other than the indicator flag to avoid error handling
       * issues (e.g. IO exceptions occuring in two different threads.
       * Resist the urge to actually perform rotation in a separate thread!
       */
      rollService.scheduleAtFixedRate(new Runnable() {

        @Override
        public void run() {
          logger.debug("Marking time to rotate file {}",
              pathController.getCurrentFile());//产生一个新的文件
          shouldRotate = true;
        }

      }, rollInterval, rollInterval, TimeUnit.SECONDS);
    } else {
      logger.info("RollInterval is not valid, file rolling will not happen.");//说明文件不会被滚动起来
    }
    logger.info("RollingFileSink {} started.", getName());
  }

  @Override
  public Status process() throws EventDeliveryException {
    if (shouldRotate) {//true说明要往新文件里面写数据了,即文件已经滚动切换了
      logger.debug("Time to rotate {}", pathController.getCurrentFile());

      if (outputStream != null) {//说明文件已经存在,要进行关闭
        logger.debug("Closing file {}", pathController.getCurrentFile());

        try {
          serializer.flush();
          serializer.beforeClose();
          outputStream.close();
          sinkCounter.incrementConnectionClosedCount();
          shouldRotate = false;//说明已经切换完毕
        } catch (IOException e) {
          sinkCounter.incrementConnectionFailedCount();
          throw new EventDeliveryException("Unable to rotate file "
              + pathController.getCurrentFile() + " while delivering event", e);
        } finally {
          serializer = null;
          outputStream = null;
        }
        pathController.rotate();
      }
    }

    if (outputStream == null) {
      File currentFile = pathController.getCurrentFile();//获取当前要写入的文件
      logger.debug("Opening output stream for file {}", currentFile);
      try {
        outputStream = new BufferedOutputStream(
            new FileOutputStream(currentFile));
        serializer = EventSerializerFactory.getInstance(
            serializerType, serializerContext, outputStream);//序列化后的数据写出到哪个流中
        serializer.afterCreate();//文件第一次写入数据的时候,即刚刚创建完成后,要先写入一些头信息
        sinkCounter.incrementConnectionCreatedCount();
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        throw new EventDeliveryException("Failed to open file "
            + pathController.getCurrentFile() + " while delivering event", e);
      }
    }

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;

    try {
      transaction.begin();
      int eventAttemptCounter = 0;
      for (int i = 0; i < batchSize; i++) {//循环写入多少条事件数据
        event = channel.take();
        if (event != null) {
          sinkCounter.incrementEventDrainAttemptCount();
          eventAttemptCounter++;
          serializer.write(event);

          /*
           * FIXME: Feature: Rotate on size and time by checking bytes written and
           * setting shouldRotate = true if we're past a threshold.
           */

          /*
           * FIXME: Feature: Control flush interval based on time or number of
           * events. For now, we're super-conservative and flush on each write.
           */
        } else {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          break;
        }
      }
      serializer.flush();
      outputStream.flush();
      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to process transaction", ex);
    } finally {
      transaction.close();
    }

    return result;
  }

  @Override
  public void stop() {
    logger.info("RollingFile sink {} stopping...", getName());
    sinkCounter.stop();
    super.stop();

    if (outputStream != null) {
      logger.debug("Closing file {}", pathController.getCurrentFile());

      try {
        serializer.flush();
        serializer.beforeClose();
        outputStream.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Unable to close output stream. Exception follows.", e);
      } finally {
        outputStream = null;
        serializer = null;
      }
    }
    if (rollInterval > 0) {
      rollService.shutdown();

      while (!rollService.isTerminated()) {
        try {
          rollService.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          logger.debug("Interrupted while waiting for roll service to stop. " +
                       "Please report this.", e);
        }
      }
    }
    logger.info("RollingFile sink {} stopped. Event metrics: {}",
        getName(), sinkCounter);
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }

  public long getRollInterval() {
    return rollInterval;
  }

  public void setRollInterval(long rollInterval) {
    this.rollInterval = rollInterval;
  }

}
