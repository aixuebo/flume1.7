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
package org.apache.flume.channel.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.conf.LogPrivacyUtil;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.*;
import static scala.collection.JavaConverters.asJavaListConverter;


/**
 * put操作,从header中获取存储到哪个partition中,key也从header中获取,将数据存储到内存的队列中,当commit的时候在一起提交给kafka-----因为put都在内存,所以回滚就直接情况内存即可
 * take操作,从kafka中读取数据,转换成事件,存储在内存队列中,当commit的时候,提交offset到kafka即可---因为take的数据没有改变kafka的offset,因此回滚的时候什么也不用做
 *
 * 可以在多个source节点上配置,让source的数据每一个节点上的数据直接写入到kafka的对应的partition上,比如有10个partition,那么可以容纳N多个source节点写入数据
 * 同时在多个sink节点配置,让sink消费kafka上的数据,这样channel就强大了
 * 因为增加一个souce节点的生产者,他也会向指定partition发送信息,不会影响kafka集群,但是增加一个sink,会导致消费者的partition重新分配,比如原有3个sink,每一个sink分配了3个partition,现在增加了一个sink,因此其中有的sink以前消费partitionX,现在就不能再消费了，如果在消费就变成重复消费了。
 * 因此一旦感知sink节点增加了,则将rebalanceFlag设置为true,因此take获取数据的时候就不会在获取了,下一次重新加载消费者,因此避免重复消费问题。
 */
public class KafkaChannel extends BasicChannelSemantics {

  private static final Logger logger =
          LoggerFactory.getLogger(KafkaChannel.class);

  // Constants used only for offset migration zookeeper connections
  private static final int ZK_SESSION_TIMEOUT = 30000;
  private static final int ZK_CONNECTION_TIMEOUT = 30000;

  //消费者和生产者的配置信息
  private final Properties consumerProps = new Properties();
  private final Properties producerProps = new Properties();

  private KafkaProducer<String, byte[]> producer;//生产者对象,key是String,value是字节数组
  private final String channelUUID = UUID.randomUUID().toString();//表示该渠道的唯一标识

  private AtomicReference<String> topic = new AtomicReference<String>();
  private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
  private String zookeeperConnect = null;//zookeeper连接串

  //默认topic以及组
  private String topicStr = DEFAULT_TOPIC;
  private String groupId = DEFAULT_GROUP_ID;
  private String partitionHeader = null;//header中获取partition的key
  private Integer staticPartitionId;//静态的向哪个partition写入数据
  private boolean migrateZookeeperOffsets = DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS;//true表示说明位置存储在zookeeper上了

  //used to indicate if a rebalance has occurred during the current transaction
  AtomicBoolean rebalanceFlag = new AtomicBoolean();
  //This isn't a Kafka property per se, but we allow it to be configurable
  private long pollTimeout = DEFAULT_POLL_TIMEOUT;


  // Track all consumers to close them eventually.
  //持有所有的消费者
  private final List<ConsumerAndRecords> consumers =
          Collections.synchronizedList(new LinkedList<ConsumerAndRecords>());

  private KafkaChannelCounter counter;

  /* Each Consumer commit will commit all partitions owned by it. To
  * ensure that each partition is only committed when all events are
  * actually done, we will need to keep a Consumer per thread.
  * 本线程级别的消费者
  */
  private final ThreadLocal<ConsumerAndRecords> consumerAndRecords =
      new ThreadLocal<ConsumerAndRecords>() {
        @Override
        public ConsumerAndRecords initialValue() {
          return createConsumerAndRecords();
        }
      };

  @Override
  public void start() {
    logger.info("Starting Kafka Channel: {}", getName());
    // As a migration step check if there are any offsets from the group stored in kafka
    // If not read them from Zookeeper and commit them to Kafka
    if (migrateZookeeperOffsets && zookeeperConnect != null && !zookeeperConnect.isEmpty()) {//说明要加载zookeeper,获取每一个topic-partition的读取位置
      migrateOffsets();
    }
    producer = new KafkaProducer<String, byte[]>(producerProps);
    // We always have just one topic being read by one thread
    logger.info("Topic = {}", topic.get());
    counter.start();
    super.start();
  }

  @Override
  public void stop() {
    for (ConsumerAndRecords c : consumers) {
      try {
        decommissionConsumerAndRecords(c);
      } catch (Exception ex) {
        logger.warn("Error while shutting down consumer.", ex);
      }
    }
    producer.close();
    counter.stop();
    super.stop();
    logger.info("Kafka channel {} stopped. Metrics: {}", getName(),
            counter);
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new KafkaTransaction();
  }

  @Override
  public void configure(Context ctx) {

    //Can remove in the next release
    translateOldProps(ctx);

    topicStr = ctx.getString(TOPIC_CONFIG);
    if (topicStr == null || topicStr.isEmpty()) {
      topicStr = DEFAULT_TOPIC;
      logger.info("Topic was not specified. Using {} as the topic.", topicStr);
    }
    topic.set(topicStr);

    groupId = ctx.getString(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
    if (groupId == null || groupId.isEmpty()) {
      groupId = DEFAULT_GROUP_ID;
      logger.info("Group ID was not specified. Using {} as the group id.", groupId);
    }

    //创建broker节点集合
    String bootStrapServers = ctx.getString(BOOTSTRAP_SERVERS_CONFIG);
    if (bootStrapServers == null || bootStrapServers.isEmpty()) {
      throw new ConfigurationException("Bootstrap Servers must be specified");
    }

    //配置生产者和消费者配置信息
    setProducerProps(ctx, bootStrapServers);
    setConsumerProps(ctx, bootStrapServers);

    parseAsFlumeEvent = ctx.getBoolean(PARSE_AS_FLUME_EVENT, DEFAULT_PARSE_AS_FLUME_EVENT);
    pollTimeout = ctx.getLong(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);

    staticPartitionId = ctx.getInteger(STATIC_PARTITION_CONF);
    partitionHeader = ctx.getString(PARTITION_HEADER_NAME);

    migrateZookeeperOffsets = ctx.getBoolean(MIGRATE_ZOOKEEPER_OFFSETS,
      DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS);
    zookeeperConnect = ctx.getString(ZOOKEEPER_CONNECT_FLUME_KEY);

    if (logger.isDebugEnabled() && LogPrivacyUtil.allowLogPrintConfig()) {
      logger.debug("Kafka properties: {}", ctx);
    }

    if (counter == null) {
      counter = new KafkaChannelCounter(getName());
    }
  }

  // We can remove this once the properties are officially deprecated
  //老配置属性转换成新的配置
  private void translateOldProps(Context ctx) {

    if (!(ctx.containsKey(TOPIC_CONFIG))) {//配置topic的name
      ctx.put(TOPIC_CONFIG, ctx.getString("topic"));
      logger.warn("{} is deprecated. Please use the parameter {}", "topic", TOPIC_CONFIG);
    }

    //Broker List
    // If there is no value we need to check and set the old param and log a warning message
    if (!(ctx.containsKey(BOOTSTRAP_SERVERS_CONFIG))) {
      String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
      if (brokerList == null || brokerList.isEmpty()) {
        throw new ConfigurationException("Bootstrap Servers must be specified");
      } else {
        ctx.put(BOOTSTRAP_SERVERS_CONFIG, brokerList);
        logger.warn("{} is deprecated. Please use the parameter {}",
                    BROKER_LIST_FLUME_KEY, BOOTSTRAP_SERVERS_CONFIG);
      }
    }

    //GroupId
    // If there is an old Group Id set, then use that if no groupId is set.
    if (!(ctx.containsKey(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG))) {//配置使用的组ID
      String oldGroupId = ctx.getString(GROUP_ID_FLUME);
      if (oldGroupId != null  && !oldGroupId.isEmpty()) {
        ctx.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG, oldGroupId);
        logger.warn("{} is deprecated. Please use the parameter {}",
                    GROUP_ID_FLUME, KAFKA_CONSUMER_PREFIX + ConsumerConfig.GROUP_ID_CONFIG);
      }
    }

    //从哪里开始读取数据
    if (!(ctx.containsKey((KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)))) {
      Boolean oldReadSmallest = ctx.getBoolean(READ_SMALLEST_OFFSET);
      String auto;
      if (oldReadSmallest != null) {
        if (oldReadSmallest) {
          auto = "earliest";
        } else {
          auto = "latest";
        }
        ctx.put(KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,auto);
        logger.warn("{} is deprecated. Please use the parameter {}",
                    READ_SMALLEST_OFFSET,
                    KAFKA_CONSUMER_PREFIX + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
      }

    }
  }

  //配置生产者属性
  private void setProducerProps(Context ctx, String bootStrapServers) {
    producerProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);//设置生产者ack生产数据的回复模式
    //如何序列化
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
    //Defaults overridden based on config
    producerProps.putAll(ctx.getSubProperties(KAFKA_PRODUCER_PREFIX));//获取生产者的配置信息
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);//设置brokerList集合
  }

  protected Properties getProducerProps() {
    return producerProps;
  }

  //配置消费者属性
  private void setConsumerProps(Context ctx, String bootStrapServers) {
    //如何反序列化
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIAIZER);
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);//默认从哪里开始消费
    //Defaults overridden based on config
    consumerProps.putAll(ctx.getSubProperties(KAFKA_CONSUMER_PREFIX));//获取消费者的配置信息
    //These always take precedence over config
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);//设置brokerList集合
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);//设置消费者组
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);//不自动提交commit
  }

  protected Properties getConsumerProps() {
    return consumerProps;
  }

  //创建一个消费者
  private synchronized ConsumerAndRecords createConsumerAndRecords() {
    try {
      KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerProps);
      ConsumerAndRecords car = new ConsumerAndRecords(consumer, channelUUID);
      logger.info("Created new consumer to connect to Kafka");
      car.consumer.subscribe(Arrays.asList(topic.get()),
                             new ChannelRebalanceListener(rebalanceFlag));//注册一个监听器,说明该消费者监听一个信息
      car.offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
      consumers.add(car);
      return car;
    } catch (Exception e) {
      throw new FlumeException("Unable to connect to Kafka", e);
    }
  }

  //说明位置存储在zookeeper上了
  //说明要加载zookeeper,获取每一个topic-partition的读取位置
  private void migrateOffsets() {
    ZkUtils zkUtils = ZkUtils.apply(zookeeperConnect, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT,
        JaasUtils.isZkSecurityEnabled());//连接zookeeper
    KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
    try {
      Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = getKafkaOffsets(consumer);//获取每一个topic-partition的位置
      if (!kafkaOffsets.isEmpty()) {
        logger.info("Found Kafka offsets for topic " + topicStr +
            ". Will not migrate from zookeeper");
        logger.debug("Offsets found: {}", kafkaOffsets);
        return;
      }

      logger.info("No Kafka offsets found. Migrating zookeeper offsets");
      Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets = getZookeeperOffsets(zkUtils);
      if (zookeeperOffsets.isEmpty()) {
        logger.warn("No offsets to migrate found in Zookeeper");
        return;
      }

      logger.info("Committing Zookeeper offsets to Kafka");
      logger.debug("Offsets to commit: {}", zookeeperOffsets);
      consumer.commitSync(zookeeperOffsets);
      // Read the offsets to verify they were committed
      Map<TopicPartition, OffsetAndMetadata> newKafkaOffsets = getKafkaOffsets(consumer);//提交后再读取kafka的位置信息
      logger.debug("Offsets committed: {}", newKafkaOffsets);
      if (!newKafkaOffsets.keySet().containsAll(zookeeperOffsets.keySet())) {//说明有一些topic-partition没有提交
        throw new FlumeException("Offsets could not be committed");
      }
    } finally {
      zkUtils.close();
      consumer.close();
    }
  }

  //消费者对象读取kafka在zookeeper上的信息,获取每一个topic-partition读取到哪个位置了
    //参数是消费者客户端对象
  private Map<TopicPartition, OffsetAndMetadata> getKafkaOffsets(
      KafkaConsumer<String, byte[]> client) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    List<PartitionInfo> partitions = client.partitionsFor(topicStr);//读取topic的所有的partition元数据信息集合
    for (PartitionInfo partition : partitions) {
      TopicPartition key = new TopicPartition(topicStr, partition.partition());//获取每一个topic-partition的信息
      OffsetAndMetadata offsetAndMetadata = client.committed(key);
      if (offsetAndMetadata != null) {
        offsets.put(key, offsetAndMetadata);
      }
    }
    return offsets;
  }

  private Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(ZkUtils client) {
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, topicStr);//获取该topic被该组消费的情况
    List<String> partitions = asJavaListConverter(
        client.getChildrenParentMayNotExist(topicDirs.consumerOffsetDir())).asJava();
    for (String partition : partitions) {
      TopicPartition key = new TopicPartition(topicStr, Integer.valueOf(partition));
      Option<String> data = client.readDataMaybeNull(
          topicDirs.consumerOffsetDir() + "/" + partition)._1();
      if (data.isDefined()) {
        Long offset = Long.valueOf(data.get());
        offsets.put(key, new OffsetAndMetadata(offset));
      }
    }
    return offsets;
  }

  //关闭一个消费者
  private void decommissionConsumerAndRecords(ConsumerAndRecords c) {
    c.consumer.close();
  }

  @VisibleForTesting
  void registerThread() {
    try {
      consumerAndRecords.get();
    } catch (Exception e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }
  }

  //说明该事务是take的还是put操作的
  private enum TransactionType {
    PUT,
    TAKE,
    NONE
  }

  //表示一个事务对象
  private class KafkaTransaction extends BasicTransactionSemantics {

    private TransactionType type = TransactionType.NONE;
    private Optional<ByteArrayOutputStream> tempOutStream = Optional
            .absent();//临时的输出文件流
    // For put transactions, serialize the events and hold them until the commit goes is requested.
    private Optional<LinkedList<ProducerRecord<String, byte[]>>> producerRecords =
        Optional.absent();//生产者存储的集合---先缓存,当commit的时候在提交到kafka
    // For take transactions, deserialize and hold them till commit goes through
    private Optional<LinkedList<Event>> events = Optional.absent();//用于take过程中获取到的事件集合
    private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer =
            Optional.absent();//存储的value是avro对象格式的二进制字节数组
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader =
            Optional.absent();
    private Optional<LinkedList<Future<RecordMetadata>>> kafkaFutures =
            Optional.absent();//当commit的时候,记录每一个send发出去的每一条数据,然后用于future模式,确保都提交到kafka成功
    private final String batchUUID = UUID.randomUUID().toString();//表示事务的唯一ID

    // Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;
    private BinaryDecoder decoder = null;
    private boolean eventTaken = false;

    @Override
    protected void doBegin() throws InterruptedException {
      rebalanceFlag.set(false);
    }

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;//说明是put操作
      if (!producerRecords.isPresent()) {//生产者存储的集合必须存在
        producerRecords = Optional.of(new LinkedList<ProducerRecord<String, byte[]>>());
      }
      String key = event.getHeaders().get(KEY_HEADER);

      Integer partitionId = null;
      try {
        if (staticPartitionId != null) {
          partitionId = staticPartitionId;
        }
        //Allow a specified header to override a static ID
        if (partitionHeader != null) {
          String headerVal = event.getHeaders().get(partitionHeader);
          if (headerVal != null) {
            partitionId = Integer.parseInt(headerVal);
          }
        }
        if (partitionId != null) {
          producerRecords.get().add(
              new ProducerRecord<String, byte[]>(topic.get(), partitionId, key,
                                                 serializeValue(event, parseAsFlumeEvent)));
        } else {
          producerRecords.get().add(
              new ProducerRecord<String, byte[]>(topic.get(), key,
                                                 serializeValue(event, parseAsFlumeEvent)));
        }
      } catch (NumberFormatException e) {
        throw new ChannelException("Non integer partition id specified", e);
      } catch (Exception e) {
        throw new ChannelException("Error while serializing event", e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Event doTake() throws InterruptedException {
      type = TransactionType.TAKE;//说明是take操作的事务
      try {
        if (!(consumerAndRecords.get().uuid.equals(channelUUID))) {//说明渠道不相同.有问题了,因此要关闭该消费者
          logger.info("UUID mismatch, creating new consumer");
          decommissionConsumerAndRecords(consumerAndRecords.get());
          consumerAndRecords.remove();
        }
      } catch (Exception ex) {
        logger.warn("Error while shutting down consumer", ex);
      }
      if (!events.isPresent()) {
        events = Optional.of(new LinkedList<Event>());
      }
      Event e;
      // Give the channel a chance to commit if there has been a rebalance
      if (rebalanceFlag.get()) {//说明此时平衡了,不能在take数据了
        logger.debug("Returning null event after Consumer rebalance.");
        return null;
      }
      if (!consumerAndRecords.get().failedEvents.isEmpty()) {//从以前的数据中获取一个数据
        e = consumerAndRecords.get().failedEvents.removeFirst();
      } else {

        if (logger.isDebugEnabled()) {
          logger.debug("Assigment: {}", consumerAndRecords.get().consumer.assignment().toString());
        }

        try {
          long startTime = System.nanoTime();
          if (!consumerAndRecords.get().recordIterator.hasNext()) {//继续获取下一批的事件
            consumerAndRecords.get().poll();
          }
          if (consumerAndRecords.get().recordIterator.hasNext()) {//说明可以take到数据
            ConsumerRecord<String, byte[]> record = consumerAndRecords.get().recordIterator.next();//获取一个元素
            e = deserializeValue(record.value(), parseAsFlumeEvent);//反序列化该事件
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1, batchUUID);
            consumerAndRecords.get().offsets.put(tp, oam);

            if (logger.isTraceEnabled()) {
              logger.trace("Took offset: {}", consumerAndRecords.get().offsets.toString());
            }

            //Add the key to the header
            if (record.key() != null) {
              e.getHeaders().put(KEY_HEADER, record.key());
            }

            if (logger.isDebugEnabled()) {
              logger.debug("Processed output from partition {} offset {}",
                           record.partition(), record.offset());
            }

            long endTime = System.nanoTime();
            counter.addToKafkaEventGetTimer((endTime - startTime) / (1000 * 1000));
          } else {//说明kafka拿不到数据了
            return null;
          }
        } catch (Exception ex) {
          logger.warn("Error while getting events from Kafka. This is usually caused by " +
                      "trying to read a non-flume event. Ensure the setting for " +
                      "parseAsFlumeEvent is correct", ex);
          throw new ChannelException("Error while getting events from Kafka", ex);
        }
      }
      eventTaken = true;
      events.get().add(e);
      return e;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {//提交put事务
        if (!kafkaFutures.isPresent()) {//当commit的时候,记录每一个send发出去的每一条数据,然后用于future模式,确保都提交到kafka成功
          kafkaFutures = Optional.of(new LinkedList<Future<RecordMetadata>>());
        }
        try {
          long batchSize = producerRecords.get().size();//要提交给kafka的数据数量
          long startTime = System.nanoTime();
          int index = 0;
          for (ProducerRecord<String, byte[]> record : producerRecords.get()) {//循环每一个要提交的数据
            index++;
            kafkaFutures.get().add(producer.send(record, new ChannelCallback(index, startTime)));//发送数据后.成功后产生一个回调函数
          }
          //prevents linger.ms from being a problem
          producer.flush();

          for (Future<RecordMetadata> future : kafkaFutures.get()) {//阻塞每一个提交的请求
            future.get();
          }
          long endTime = System.nanoTime();
          counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
          counter.addToEventPutSuccessCount(batchSize);
          producerRecords.get().clear();
          kafkaFutures.get().clear();
        } catch (Exception ex) {
          logger.warn("Sending events to Kafka failed", ex);
          throw new ChannelException("Commit failed as send to Kafka failed",
                  ex);
        }
      } else {//对take事务提交
        if (consumerAndRecords.get().failedEvents.isEmpty() && eventTaken) {
          long startTime = System.nanoTime();
          consumerAndRecords.get().commitOffsets();//对take的offset数据信息 提交给zookeeper
          long endTime = System.nanoTime();
          counter.addToKafkaCommitTimer((endTime - startTime) / (1000 * 1000));
          consumerAndRecords.get().printCurrentAssignment();
        }
        counter.addToEventTakeSuccessCount(Long.valueOf(events.get().size()));
        events.get().clear();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {//因为put都在内存,所以回滚就直接情况内存即可
        producerRecords.get().clear();
        kafkaFutures.get().clear();
      } else {//回滚take操作
        counter.addToRollbackCounter(Long.valueOf(events.get().size()));
        //因为take的数据都已经拿到了,就是因为一些意外导致要回滚,因此虽然不影响kafka,但是也不想再次从kafka读取数据,因此将其缓冲起来
        consumerAndRecords.get().failedEvents.addAll(events.get());
        events.get().clear();
      }
    }

    //对事件进行序列化,然后将序列化的结果存储到kafka中
    //参数parseAsFlumeEvent是false,则直接使用事件的data数据即可,如是true,则要解析事件
    private byte[] serializeValue(Event event, boolean parseAsFlumeEvent) throws IOException {
      byte[] bytes;
      if (parseAsFlumeEvent) {//要解析事件
        if (!tempOutStream.isPresent()) {//创建临时输出流
          tempOutStream = Optional.of(new ByteArrayOutputStream());
        }
        if (!writer.isPresent()) {
          writer = Optional.of(new
                  SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
        }
        tempOutStream.get().reset();
        AvroFlumeEvent e = new AvroFlumeEvent(
                toCharSeqMap(event.getHeaders()),
                ByteBuffer.wrap(event.getBody()));//转换成avro事件对象
        encoder = EncoderFactory.get()
                .directBinaryEncoder(tempOutStream.get(), encoder);
        writer.get().write(e, encoder);
        encoder.flush();
        bytes = tempOutStream.get().toByteArray();
      } else {
        bytes = event.getBody();
      }
      return bytes;
    }

     //反序列化kafka的数据内容
    private Event deserializeValue(byte[] value, boolean parseAsFlumeEvent) throws IOException {
      Event e;
      if (parseAsFlumeEvent) {
        ByteArrayInputStream in =
                new ByteArrayInputStream(value);
        decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
        if (!reader.isPresent()) {
          reader = Optional.of(
                  new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
        }
        AvroFlumeEvent event = reader.get().read(null, decoder);
        e = EventBuilder.withBody(event.getBody().array(),
                toStringMap(event.getHeaders()));
      } else {
        e = EventBuilder.withBody(value, Collections.EMPTY_MAP);
      }
      return e;
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap =
            new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  /* Object to store our consumer */
  private class ConsumerAndRecords {
    final KafkaConsumer<String, byte[]> consumer;//kafka消费者客户端
    final String uuid;//该消费者属于哪个渠道
    final LinkedList<Event> failedEvents = new LinkedList<Event>();////因为take的数据都已经拿到了,就是因为一些意外导致要回滚,因此虽然不影响kafka,但是也不想再次从kafka读取数据,因此将其缓冲起来

    ConsumerRecords<String, byte[]> records;//消费的记录集合
    Iterator<ConsumerRecord<String, byte[]>> recordIterator;//消费的记录集合迭代器
    Map<TopicPartition, OffsetAndMetadata> offsets;//该消费者消费的内容--先还存在内存中

    ConsumerAndRecords(KafkaConsumer<String, byte[]> consumer, String uuid) {
      this.consumer = consumer;
      this.uuid = uuid;
      this.records = ConsumerRecords.empty();
      this.recordIterator = records.iterator();
    }

    void poll() {
      this.records = consumer.poll(pollTimeout);//继续获取下一批的事件
      this.recordIterator = records.iterator();
      logger.trace("polling");
    }

    void commitOffsets() {
      this.consumer.commitSync(offsets);
    }//提交给kafka

    // This will reset the latest assigned partitions to the last committed offsets;

    public void printCurrentAssignment() {
      StringBuilder sb = new StringBuilder();
      for (TopicPartition tp : this.consumer.assignment()) {
        try {
          sb.append("Committed: [").append(tp).append(",")
              .append(this.consumer.committed(tp).offset())
              .append(",").append(this.consumer.committed(tp).metadata()).append("]");
          if (logger.isDebugEnabled()) {
            logger.debug(sb.toString());
          }
        } catch (NullPointerException npe) {
          if (logger.isDebugEnabled()) {
            logger.debug("Committed {}", tp);
          }
        }
      }
    }
  }
}

// Throw exception if there is an error
//生产者send每一条数据后,产生一个回调函数
class ChannelCallback implements Callback {
  private static final Logger log = LoggerFactory.getLogger(ChannelCallback.class);
  private int index;//该数据是该事务的第几个数据
  private long startTime;//该事务的send的时候的时间

  public ChannelCallback(int index, long startTime) {
    this.index = index;
    this.startTime = startTime;
  }

  //记录日志说明有异常了
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (exception != null) {
      log.trace("Error sending message to Kafka due to " + exception.getMessage());
    }
    if (log.isDebugEnabled()) {
      long batchElapsedTime = System.currentTimeMillis() - startTime;
      log.debug("Acked message_no " + index + ": " + metadata.topic() + "-" +
                metadata.partition() + "-" + metadata.offset() + "-" + batchElapsedTime);
    }
  }
}

//渠道去监听topic的监听器
class ChannelRebalanceListener implements ConsumerRebalanceListener {
  private static final Logger log = LoggerFactory.getLogger(ChannelRebalanceListener.class);
  private AtomicBoolean rebalanceFlag;

  public ChannelRebalanceListener(AtomicBoolean rebalanceFlag) {
    this.rebalanceFlag = rebalanceFlag;
  }

  // Set a flag that a rebalance has occurred. Then we can commit the currently written transactions
  // on the next doTake() pass.
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
      rebalanceFlag.set(true);
    }
  }

  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    for (TopicPartition partition : partitions) {
      log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
    }
  }
}
