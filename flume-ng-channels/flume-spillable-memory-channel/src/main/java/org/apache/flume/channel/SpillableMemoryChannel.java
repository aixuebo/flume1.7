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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.instrumentation.ChannelCounter;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * SpillableMemoryChannel will use main memory for buffering events until it has reached capacity.
 * Thereafter file channel will be used as overflow.
 * </p>
 * 使用内存去存储数据,但是当达到一定空间时候,要写入到文件中
 *
 * 内存队列和文件队列一次事务只能从一个地方进行take,不能相互切换take
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
@Recyclable
public class SpillableMemoryChannel extends FileChannel {
  // config settings
  /**
   * Max number of events to be stored in memory
   */
  public static final String MEMORY_CAPACITY = "memoryCapacity";//内存队列的容量的配置
  /**
   * Seconds to wait before enabling disk overflow when memory fills up
   */
  public static final String OVERFLOW_TIMEOUT = "overflowTimeout";
  /**
   * Internal use only. To remain undocumented in User guide. Determines the
   * percent free space available in mem queue when we stop spilling to overflow
   */
  public static final String OVERFLOW_DEACTIVATION_THRESHOLD
      = "overflowDeactivationThreshold";
  /**
   * percent of buffer between byteCapacity and the estimated event size.
   */
  public static final String BYTE_CAPACITY_BUFFER_PERCENTAGE
      = "byteCapacityBufferPercentage";

  /**
   * max number of bytes used for all events in the queue.
   */
  public static final String BYTE_CAPACITY = "byteCapacity";
  /**
   * max number of events in overflow.
   */
  public static final String OVERFLOW_CAPACITY = "overflowCapacity";
  /**
   * file channel setting that is overriden by Spillable Channel
   */
  public static final String KEEP_ALIVE = "keep-alive";
  /**
   * file channel capacity overridden by Spillable Channel
   */
  public static final String CAPACITY = "capacity";
  /**
   * Estimated average size of events expected to be in the channel
   */
  public static final String AVG_EVENT_SIZE = "avgEventSize";

  private static Logger LOGGER = LoggerFactory.getLogger(SpillableMemoryChannel.class);
  public static final int defaultMemoryCapacity = 10000;//内存队列的容量默认大小
  public static final int defaultOverflowCapacity = 100000000;

  public static final int defaultOverflowTimeout = 3;
  public static final int defaultOverflowDeactivationThreshold = 5; // percent 闲置默认是5%

  // memory consumption control
  private static final int defaultAvgEventSize = 500;
  private static final Long defaultByteCapacity
      = (long) (Runtime.getRuntime().maxMemory() * .80);
  private static final int defaultByteCapacityBufferPercentage = 20;

  private volatile int byteCapacity;
  private volatile double avgEventSize = defaultAvgEventSize;
  private volatile int lastByteCapacity;
  private volatile int byteCapacityBufferPercentage;
  private Semaphore bytesRemaining;

  // for synchronizing access to primary/overflow channels & drain order
  private final Object queueLock = new Object();

  @GuardedBy(value = "queueLock")
  public ArrayDeque<Event> memQueue;

  // This semaphore tracks number of free slots in primary channel (includes
  // all active put lists) .. used to determine if the puts
  // should go into primary or overflow
  private Semaphore memQueRemaining;//内存队列中剩余的事件数量

  // tracks number of events in both channels. Takes will block on this
  private Semaphore totalStored;//可以take拿去到的元素数量

  private int maxMemQueueSize = 0;     // max sie of memory Queue 内存队列的元素最大值

  private boolean overflowDisabled; // if true indicates the overflow should not be used at all.true表示不允许使用文件存储

  // indicates if overflow can be used. invariant: false if overflowDisabled is true.
  private boolean overflowActivated = false;//true表示已经开启了文件存储了

  private int memoryCapacity = -1;     // max events that the channel can hold  in memory 在内存中最大的事件队列容量
  private int overflowCapacity; //在文件中的最大事件容量

  private int overflowTimeout;//这些时间内只要内存元素数量足够,都可以在内存中存储

  // mem full % at which we stop spill to overflow
  private double overflowDeactivationThreshold
      = defaultOverflowDeactivationThreshold / 100;//超过该伐值后,就可以使用内存存储元素

  public SpillableMemoryChannel() {
    super();
  }

  protected int getTotalStored() {
    return totalStored.availablePermits();
  }

  public int getMemoryCapacity() {
    return memoryCapacity;
  }

  public int getOverflowTimeout() {
    return overflowTimeout;
  }

  public int getMaxMemQueueSize() {
    return maxMemQueueSize;
  }

  protected Integer getOverflowCapacity() {
    return overflowCapacity;
  }

  protected boolean isOverflowDisabled() {
    return overflowDisabled;
  }

  @VisibleForTesting
  protected ChannelCounter channelCounter;

  public final DrainOrderQueue drainOrder = new DrainOrderQueue();

  public int queueSize() {
    synchronized (queueLock) {
      return memQueue.size();
    }
  }

  private static class MutableInteger {
    private int value;

    public MutableInteger(int val) {
      value = val;
    }

    public void add(int amount) {
      value += amount;
    }

    public int intValue() {
      return value;
    }
  }

  //  pop on a empty queue will throw NoSuchElementException
  // invariant: 0 will never be left in the queue
  //一个顺序队列
  public static class DrainOrderQueue {

    //如果元素大于0.表示存储的是放内存的,如果文件小于0,说明存储的内容是存储到文件中的
    public ArrayDeque<MutableInteger> queue = new ArrayDeque<MutableInteger>(1000);//一个队列---从后面put,从前面takle

    public int totalPuts = 0;  // for debugging only 总put数量,一直追加不会更改,表示一共写入了多少个元素,用于debug或者统计分析
    private long overflowCounter = 0; // # of items in overflow channel 添加到文件中的put数量,此时还有多少个元素在文件中,实施更改的

    //打印队列内容
    public String dump() {
      StringBuilder sb = new StringBuilder();

      sb.append("  [ ");
      for (MutableInteger i : queue) {
        sb.append(i.intValue());
        sb.append(" ");
      }
      sb.append("]");
      return sb.toString();
    }

    //存放到内存中,添加到尾巴里面
    public void putPrimary(Integer eventCount) {
      totalPuts += eventCount;
      if ((queue.peekLast() == null) || queue.getLast().intValue() < 0) {//添加的最后一个元素,不是length位置的元素,说明此时小于0,说明是记录存储到文件的,因此要重新创建一个元素
        queue.addLast(new MutableInteger(eventCount));
      } else {
        queue.getLast().add(eventCount);
      }
    }

    //存放内存中,添加到头里面  //take还原的时候,将元素数量追加到头里面去
    public void putFirstPrimary(Integer eventCount) {
      if ((queue.peekFirst() == null) || queue.getFirst().intValue() < 0) {//添加的第一个元素
        queue.addFirst(new MutableInteger(eventCount));
      } else {
        queue.getFirst().add(eventCount);
      }
    }

    //添加到文件中
    public void putOverflow(Integer eventCount) {
      totalPuts += eventCount;
      if ((queue.peekLast() == null) || queue.getLast().intValue() > 0) {//说明最后一个元素是正数,表示是存储的内存数量
        queue.addLast(new MutableInteger(-eventCount));//因此新增一个文件数量
      } else {
        queue.getLast().add(-eventCount);//说明此时存储的就是文件数量,因此继续追加
      }
      overflowCounter += eventCount;
    }

    //take还原的时候,将元素数量追加到头里面去
    public void putFirstOverflow(Integer eventCount) {
      if ((queue.peekFirst() == null) || queue.getFirst().intValue() > 0) {
        queue.addFirst(new MutableInteger(-eventCount));
      } else {
        queue.getFirst().add(-eventCount);
      }
      overflowCounter += eventCount;
    }

    //获取头部有多少个事件
    public int front() {
      return queue.getFirst().intValue();
    }

    public boolean isEmpty() {
      return queue.isEmpty();
    }

    //从内存中take元素,准备获取n个元素
    public void takePrimary(int takeCount) {
      MutableInteger headValue = queue.getFirst();

      // this condition is optimization to avoid redundant conversions of
      // int -> Integer -> string in hot path
      if (headValue.intValue() < takeCount) {//说明不能拿到这么多元素,因为没有这么多元素
        throw new IllegalStateException("Cannot take " + takeCount +
            " from " + headValue.intValue() + " in DrainOrder Queue");
      }

      //头部更新数量
      headValue.add(-takeCount);
      if (headValue.intValue() == 0) {//如果是0,则从中删除
        queue.removeFirst();
      }
    }

    //从文件中take,因为存储的是负数
    public void takeOverflow(int takeCount) {
      MutableInteger headValue = queue.getFirst();
      if (headValue.intValue() > -takeCount) { //因为headValue.intValue()存储的是负数
        throw new IllegalStateException("Cannot take " + takeCount + " from "
            + headValue.intValue() + " in DrainOrder Queue head ");
      }

      headValue.add(takeCount);//负数+一个正数,等于做减法,剩余的还是一个负数,因此表示剩余多少个元素
      if (headValue.intValue() == 0) {
        queue.removeFirst();
      }
      overflowCounter -= takeCount;
    }

  }

  //一个事务
  private class SpillableMemoryTransaction extends BasicTransactionSemantics {
    //提交给文件的put和take两个队列,这两个队列是有事务的
    BasicTransactionSemantics overflowTakeTx = null; // Take-Txn for overflow
    BasicTransactionSemantics overflowPutTx = null;  // Put-Txn for overflow
    boolean useOverflow = false;//true表示使用文件进行操作
    boolean putCalled = false;    // set on first invocation to put ,true表示执行的是put方法的事务
    boolean takeCalled = false;   // set on first invocation to take,true表示执行的是take方法的事务
    int largestTakeTxSize = 5000; // not a constraint, just hint for allocation 不是一个约束,只是提示分配
    int largestPutTxSize = 5000;  // not a constraint, just hint for allocation

    Integer overflowPutCount = 0;    // # of puts going to overflow in this Txn 提交给文件的事件数量

    //本次put和take的字节数
    private int putListByteCount = 0;
    private int takeListByteCount = 0;
    private int takeCount = 0;//本次事务内获取take的数量

    //两个临时队列
    ArrayDeque<Event> takeList;//存储内存take的元素
    ArrayDeque<Event> putList;//存储
    private final ChannelCounter channelCounter;

    public SpillableMemoryTransaction(ChannelCounter counter) {
      takeList = new ArrayDeque<Event>(largestTakeTxSize);
      putList = new ArrayDeque<Event>(largestPutTxSize);
      channelCounter = counter;
    }

    @Override
    public void begin() {
      super.begin();
    }

    @Override
    public void close() {
      if (overflowTakeTx != null) {
        overflowTakeTx.close();
      }
      if (overflowPutTx != null) {
        overflowPutTx.close();
      }
      super.close();
    }

    //临时把事件存储到内存中
    @Override
    protected void doPut(Event event) throws InterruptedException {
      channelCounter.incrementEventPutAttemptCount();

      putCalled = true;
      int eventByteSize = (int) Math.ceil(estimateEventSize(event) / avgEventSize);
      if (!putList.offer(event)) {
        throw new ChannelFullException("Put queue in " + getName() +
            " channel's Transaction having capacity " + putList.size() +
            " full, consider reducing batch size of sources");
      }
      putListByteCount += eventByteSize;
    }


    // Take will limit itself to a single channel within a transaction.
    // This ensures commits/rollbacks are restricted to a single channel.
    //只能从文件读取  或者从内存读取,一直读取到没有数据为止。
    @Override
    protected Event doTake() throws InterruptedException {
      channelCounter.incrementEventTakeAttemptCount();
      if (!totalStored.tryAcquire(overflowTimeout, TimeUnit.SECONDS)) {//true表示有数据可以被take
        LOGGER.debug("Take is backing off as channel is empty.");
        return null;
      }
      boolean takeSuceeded = false;//true表示获取元素成功
      try {
        Event event;
        synchronized (queueLock) {
          int drainOrderTop = drainOrder.front();//获取此时第一个位置有多少个元素

          if (!takeCalled) {//初始化第一次take
            takeCalled = true;
            if (drainOrderTop < 0) {//说明是存储文件里面了
              useOverflow = true;//用于设置此次事务读取的是文件还是内存
              overflowTakeTx = getOverflowTx();//开启获取文件的take事务
              overflowTakeTx.begin();
            }
          }

          if (useOverflow) {//说明使用文件
            if (drainOrderTop > 0) {//一定是负数,正数表示从内存中读取数据---说明没有数据了,不能读取了,要重新开启事务去读取
              LOGGER.debug("Take is switching to primary");
              return null;       // takes should now occur from primary channel
            }

            event = overflowTakeTx.take();
            ++takeCount;
            drainOrder.takeOverflow(1);//说明已经拿到了一个元素
          } else {//说明从内存中获取元素
            if (drainOrderTop < 0) {//此时表示从文件中获取元素,因此有问题
              LOGGER.debug("Take is switching to overflow");
              return null;      // takes should now occur from overflow channel
            }

            event = memQueue.poll();
            ++takeCount;
            drainOrder.takePrimary(1);//表示从内存中获取了一个元素
            Preconditions.checkNotNull(event, "Queue.poll returned NULL despite"
                + " semaphore signalling existence of entry");
          }
        }

        int eventByteSize = (int) Math.ceil(estimateEventSize(event) / avgEventSize);
        if (!useOverflow) {//false 表示从内存中take
          // takeList is thd pvt, so no need to do this in synchronized block
          takeList.offer(event);//因此将数据添加到take的队列中
        }

        takeListByteCount += eventByteSize;
        takeSuceeded = true;
        return event;
      } finally {
        if (!takeSuceeded) {
          totalStored.release();
        }
      }
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (putCalled) {
        putCommit();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Put Committed. Drain Order Queue state : " + drainOrder.dump());
        }
      } else if (takeCalled) {
        takeCommit();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Take Committed. Drain Order Queue state : " + drainOrder.dump());
        }
      }
    }

    //提交
    private void takeCommit() {
      if (takeCount > largestTakeTxSize) {
        largestTakeTxSize = takeCount;
      }

      synchronized (queueLock) {
        if (overflowTakeTx != null) {//说明从文件里面获取take了
          overflowTakeTx.commit();
        }
        double memoryPercentFree = (memoryCapacity == 0) ? 0 ://memoryCapacity=0,说明不允许使用内存存储
            (memoryCapacity - memQueue.size() + takeCount) / (double) memoryCapacity;//刨除本次take的元素后,剩余位置占用总队列的比例.表示空闲比例

        //已经开启了文件存储了,但是此时内存的限制比例比配置的好,因此可以切换到内存去存储
        if (overflowActivated && memoryPercentFree >= overflowDeactivationThreshold) {
          overflowActivated = false;
          LOGGER.info("Overflow Deactivated");
        }
        channelCounter.setChannelSize(getTotalStored());
      }
      if (!useOverflow) {//false说明使用的是内存
        memQueRemaining.release(takeCount);//还原内存位置
        bytesRemaining.release(takeListByteCount);//还原内存的字节限制
      }

      channelCounter.addToEventTakeSuccessCount(takeCount);
    }

    private void putCommit() throws InterruptedException {
      // decide if overflow needs to be used
      int timeout = overflowActivated ? 0 : overflowTimeout; //如果已经开始文件存储了,因此是不需要等待的,直接存储即可,所以时间是0

      if (memoryCapacity != 0) {//说明内存大小是有限制的
        //双重减少,减少内存队列的数量,减少字节数量
        // check for enough event slots(memoryCapacity) for using memory queue
        if (!memQueRemaining.tryAcquire(putList.size(), timeout,
            TimeUnit.SECONDS)) {//在内存队列中获取是否能够存储这些put元素
            //进入if,说明内存已经不够了
          if (overflowDisabled) {//又不允许使用文件存储,因此抛异常
            throw new ChannelFullException("Spillable Memory Channel's " +
                "memory capacity has been reached and overflow is " +
                "disabled. Consider increasing memoryCapacity.");//说明内存已经达到一定伐值了,文件又没有开启
          }
          overflowActivated = true;//开启文件存储
          useOverflow = true;//说明已经溢出到文件中
        // check if we have enough byteCapacity for using memory queue
        } else if (!bytesRemaining.tryAcquire(putListByteCount,
                                              overflowTimeout, TimeUnit.SECONDS)) {//判断插入的字节数是否满足条件
          //进入if说明没有空间,但是memQueRemaining队列有空间,并且已经扣减了队列的空间数量
          memQueRemaining.release(putList.size());//则还原内存中的数量
          if (overflowDisabled) {
            throw new ChannelFullException("Spillable Memory Channel's "
                + "memory capacity has been reached.  "
                + (bytesRemaining.availablePermits() * (int) avgEventSize)
                + " bytes are free and overflow is disabled. Consider "
                + "increasing byteCapacity or capacity.");
          }
          overflowActivated = true;
          useOverflow = true;
        }
      } else {//说明内存大小是没有限制的,因此直接使用文件存储
        useOverflow = true;
      }

      if (putList.size() > largestPutTxSize) {
        largestPutTxSize = putList.size();
      }

      if (useOverflow) {
        commitPutsToOverflow();//向文件中添加
      } else {
        commitPutsToPrimary();//向内存中添加
      }
    }

      /**
       * 向文件中提交
       * 1.向文件中提交put事件
       * 2.增加可以take拿去到的元素数量
       */
    private void commitPutsToOverflow() throws InterruptedException {
      overflowPutTx = getOverflowTx();
      overflowPutTx.begin();
      for (Event event : putList) {
        overflowPutTx.put(event);
      }
      commitPutsToOverflow_core(overflowPutTx);//提交事务
      totalStored.release(putList.size());//增加可以take拿去到的元素数量
      overflowPutCount += putList.size();//提交给文件的事件数量
      channelCounter.addToEventPutSuccessCount(putList.size());
    }

    //提交事务
    private void commitPutsToOverflow_core(Transaction overflowPutTx)
        throws InterruptedException {
      // reattempt only once if overflow is full first time around 如果失败的话,可以允许尝试一次
      for (int i = 0; i < 2; ++i) {
        try {
          synchronized (queueLock) {
            overflowPutTx.commit();
            drainOrder.putOverflow(putList.size());//增加写入文件的数量
            channelCounter.setChannelSize(memQueue.size()
                + drainOrder.overflowCounter);
            break;
          }
        } catch (ChannelFullException e) { // drop lock & reattempt
          if (i == 0) {//尝试一次
            Thread.sleep(overflowTimeout * 1000);
          } else {//抛异常
            throw e;
          }
        }
      }
    }

    //放到内存里

      /**
       * 1.将数据提交给内存队列
       * 2.更新可以drainOrder队列数量
       * 3.修改最大队列的size
       * 4.增加可以take拿去到的元素数量
       */
    private void commitPutsToPrimary() {
      synchronized (queueLock) {
        for (Event e : putList) {//循环所有的元素,放到内存里面
          if (!memQueue.offer(e)) {
            throw new ChannelException("Unable to insert event into memory " +
                "queue in spite of spare capacity, this is very unexpected");
          }
        }
        drainOrder.putPrimary(putList.size());//说明放到内存里的数量
        maxMemQueueSize = (memQueue.size() > maxMemQueueSize) ? memQueue.size()
            : maxMemQueueSize;//更新内存元素的历史最大值
        channelCounter.setChannelSize(memQueue.size()
            + drainOrder.overflowCounter);
      }
      // update counters and semaphores
      totalStored.release(putList.size());//增加可以take拿去到的元素数量
      channelCounter.addToEventPutSuccessCount(putList.size());
    }

    //回滚---只能是put或者take一种形式进行回滚操作
    @Override
    protected void doRollback() {
      LOGGER.debug("Rollback() of " +
          (takeCalled ? " Take Tx" : (putCalled ? " Put Tx" : "Empty Tx")));

      if (putCalled) {//说明是put操作的回滚
        if (overflowPutTx != null) {//文件先回滚
          overflowPutTx.rollback();
        }
        if (!useOverflow) {//说明使用的是内存,因此还原使用量
          bytesRemaining.release(putListByteCount);
          //因为没有真的放到队列中,因此不用还原队列,只还原使用量即可
          putList.clear();
        }
        putListByteCount = 0;
      } else if (takeCalled) {//说明调用的是take回滚
        synchronized (queueLock) {
          if (overflowTakeTx != null) {//先回滚文件
            overflowTakeTx.rollback();
          }
          if (useOverflow) {//说明是文件
            drainOrder.putFirstOverflow(takeCount);//因此将数据还原
          } else {//因为是还原内存
            int remainingCapacity = memoryCapacity - memQueue.size();
            Preconditions.checkState(remainingCapacity >= takeCount,
                "Not enough space in memory queue to rollback takes. This" +
                    " should never happen, please report");//确保take要还原的数量要比容器能装得下,这地方总觉得有点问题,高并发下,有问题
            while (!takeList.isEmpty()) {//将获取的元素一个个添加到内存队列中
              memQueue.addFirst(takeList.removeLast());
            }
            drainOrder.putFirstPrimary(takeCount);//设置内存队列大小
          }
        }
        totalStored.release(takeCount);//增加take能获取的元素数量
      } else {
        overflowTakeTx.rollback();
      }
      channelCounter.setChannelSize(memQueue.size() + drainOrder.overflowCounter);//更新此时的队列大小
    }
  } // Transaction

  /**
   * Read parameters from context
   * <li>memoryCapacity = total number of events allowed at one time in the memory queue.内存队列的容量
   * <li>overflowCapacity = total number of events allowed at one time in the overflow file channel. 文件队列的容量
   * <li>byteCapacity = the max number of bytes used for events in the memory queue. 内存队列存储字节的容量
   * <li>byteCapacityBufferPercentage = type int. Defines the percent of buffer between byteCapacity
   *     and the estimated event size.多少百分比闲置,则可以将队列切换到内存
   * <li>overflowTimeout = type int. Number of seconds to wait on a full memory before deciding to
   *     enable overflow超时时间
   */
  @Override
  public void configure(Context context) {

    if (getLifecycleState() == LifecycleState.START ||  // does not support reconfig when running
        getLifecycleState() == LifecycleState.ERROR) {
      stop();
    }

    if (totalStored == null) {
      totalStored = new Semaphore(0);
    }

    if (channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }

    // 1) Memory Capacity
    Integer newMemoryCapacity;
    try {
      newMemoryCapacity = context.getInteger(MEMORY_CAPACITY, defaultMemoryCapacity);
      if (newMemoryCapacity == null) {
        newMemoryCapacity = defaultMemoryCapacity;
      }
      if (newMemoryCapacity < 0) {
        throw new NumberFormatException(MEMORY_CAPACITY + " must be >= 0");
      }

    } catch (NumberFormatException e) {
      newMemoryCapacity = defaultMemoryCapacity;
      LOGGER.warn("Invalid " + MEMORY_CAPACITY + " specified, initializing " +
          getName() + " channel to default value of {}", defaultMemoryCapacity);
    }
    try {
      resizePrimaryQueue(newMemoryCapacity);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // overflowTimeout - wait time before switching to overflow when mem is full
    try {
      Integer newOverflowTimeout =
          context.getInteger(OVERFLOW_TIMEOUT, defaultOverflowTimeout);
      overflowTimeout = (newOverflowTimeout != null) ? newOverflowTimeout
          : defaultOverflowTimeout;
    } catch (NumberFormatException e) {
      LOGGER.warn("Incorrect value for " + getName() + "'s " + OVERFLOW_TIMEOUT
          + " setting. Using default value {}", defaultOverflowTimeout);
      overflowTimeout = defaultOverflowTimeout;
    }

    try {
      Integer newThreshold = context.getInteger(OVERFLOW_DEACTIVATION_THRESHOLD);
      overflowDeactivationThreshold = (newThreshold != null) ?
          newThreshold / 100.0
          : defaultOverflowDeactivationThreshold / 100.0;
    } catch (NumberFormatException e) {
      LOGGER.warn("Incorrect value for " + getName() + "'s " +
              OVERFLOW_DEACTIVATION_THRESHOLD + ". Using default value {} %",
          defaultOverflowDeactivationThreshold);
      overflowDeactivationThreshold = defaultOverflowDeactivationThreshold / 100.0;
    }

    // 3) Memory consumption control
    try {
      byteCapacityBufferPercentage =
          context.getInteger(BYTE_CAPACITY_BUFFER_PERCENTAGE, defaultByteCapacityBufferPercentage);
    } catch (NumberFormatException e) {
      LOGGER.warn("Error parsing " + BYTE_CAPACITY_BUFFER_PERCENTAGE + " for "
          + getName() + ". Using default="
          + defaultByteCapacityBufferPercentage + ". " + e.getMessage());
      byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
    }

    try {
      avgEventSize = context.getInteger(AVG_EVENT_SIZE, defaultAvgEventSize);
    } catch (NumberFormatException e) {
      LOGGER.warn("Error parsing " + AVG_EVENT_SIZE + " for " + getName()
          + ". Using default = " + defaultAvgEventSize + ". "
          + e.getMessage());
      avgEventSize = defaultAvgEventSize;
    }

    try {
      byteCapacity = (int) ((context.getLong(BYTE_CAPACITY, defaultByteCapacity) *
                            (1 - byteCapacityBufferPercentage * .01)) / avgEventSize);
      if (byteCapacity < 1) {
        byteCapacity = Integer.MAX_VALUE;
      }
    } catch (NumberFormatException e) {
      LOGGER.warn("Error parsing " + BYTE_CAPACITY + " setting for " + getName()
          + ". Using default = " + defaultByteCapacity + ". "
          + e.getMessage());
      byteCapacity = (int)
          ((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01))
              / avgEventSize);
    }


    if (bytesRemaining == null) {
      bytesRemaining = new Semaphore(byteCapacity);
      lastByteCapacity = byteCapacity;
    } else {
      if (byteCapacity > lastByteCapacity) {
        bytesRemaining.release(byteCapacity - lastByteCapacity);
        lastByteCapacity = byteCapacity;
      } else {
        try {
          if (!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity,
                                         overflowTimeout, TimeUnit.SECONDS)) {
            LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, " +
                        "resizing has been aborted");
          } else {
            lastByteCapacity = byteCapacity;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    try {
      // file channel capacity
      overflowCapacity = context.getInteger(OVERFLOW_CAPACITY, defaultOverflowCapacity);
      // Determine if File channel needs to be disabled
      if (memoryCapacity < 1 && overflowCapacity < 1) {
        LOGGER.warn("For channel " + getName() + OVERFLOW_CAPACITY +
            " cannot be set to 0 if " + MEMORY_CAPACITY + " is also 0. " +
            "Using default value " + OVERFLOW_CAPACITY + " = " +
            defaultOverflowCapacity);
        overflowCapacity = defaultOverflowCapacity;
      }
      overflowDisabled = (overflowCapacity < 1);
      if (overflowDisabled) {
        overflowActivated = false;
      }
    } catch (NumberFormatException e) {
      overflowCapacity = defaultOverflowCapacity;
    }

    // Configure File channel
    context.put(KEEP_ALIVE, "0"); // override keep-alive for  File channel
    context.put(CAPACITY, Integer.toString(overflowCapacity));  // file channel capacity
    super.configure(context);
  }


  private void resizePrimaryQueue(int newMemoryCapacity) throws InterruptedException {
    if (memQueue != null && memoryCapacity == newMemoryCapacity) {
      return;
    }

    if (memoryCapacity > newMemoryCapacity) {
      int diff = memoryCapacity - newMemoryCapacity;
      if (!memQueRemaining.tryAcquire(diff, overflowTimeout, TimeUnit.SECONDS)) {
        LOGGER.warn("Memory buffer currently contains more events than the new size. " +
                    "Downsizing has been aborted.");
        return;
      }
      synchronized (queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        newQueue.addAll(memQueue);
        memQueue = newQueue;
        memoryCapacity = newMemoryCapacity;
      }
    } else {   // if (memoryCapacity <= newMemoryCapacity)
      synchronized (queueLock) {
        ArrayDeque<Event> newQueue = new ArrayDeque<Event>(newMemoryCapacity);
        if (memQueue != null) {
          newQueue.addAll(memQueue);
        }
        memQueue = newQueue;
        if (memQueRemaining == null) {
          memQueRemaining = new Semaphore(newMemoryCapacity);
        } else {
          int diff = newMemoryCapacity - memoryCapacity;
          memQueRemaining.release(diff);
        }
        memoryCapacity = newMemoryCapacity;
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    int overFlowCount = super.getDepth();
    if (drainOrder.isEmpty()) {
      drainOrder.putOverflow(overFlowCount);
      totalStored.release(overFlowCount);
    }
    channelCounter.start();
    int totalCount = overFlowCount + memQueue.size();
    channelCounter.setChannelCapacity(memoryCapacity + getOverflowCapacity());
    channelCounter.setChannelSize(totalCount);
  }

  @Override
  public synchronized void stop() {
    if (getLifecycleState() == LifecycleState.STOP) {
      return;
    }
    channelCounter.setChannelSize(memQueue.size() + drainOrder.overflowCounter);
    channelCounter.stop();
    super.stop();
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new SpillableMemoryTransaction(channelCounter);
  }

  //开启文件存储的一个事务
  private BasicTransactionSemantics getOverflowTx() {
    return super.createTransaction();
  }

  //估算事件占用的字节空间
  private long estimateEventSize(Event event) {
    byte[] body = event.getBody();
    if (body != null && body.length != 0) {
      return body.length;
    }
    //Each event occupies at least 1 slot, so return 1.
    return 1;
  }

}
