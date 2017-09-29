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
package org.apache.flume.channel.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Queue of events in the channel. This queue stores only
 * {@link FlumeEventPointer} objects which are represented
 * as 8 byte longs internally.
 *
 * 事件的队列,该队列只是存储每一个事件存储在哪个文件以及对应文件的offset,即使用一个long代替。
 *
 * Additionally the queue itself
 * of longs is stored as a memory mapped file with a fixed
 * header and circular queue semantics. The header of the queue
 * contains the timestamp of last sync, the queue size and
 * the head position.
 * 另外这个队列本身存储在内存映射文件中，该队列本身是一个循环利用的队列。
 *
 * 队列的头包含最后同步的时间戳,队列的size以及头的位置
 */
final class FlumeEventQueue {
  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeEventQueue.class);
  private static final int EMPTY = 0;
  private final EventQueueBackingStore backingStore;//数据如何存储
  private final String channelNameDescriptor;//渠道的描述
  private final InflightEventWrapper inflightTakes;//用于存储正在take操作的数据
  private final InflightEventWrapper inflightPuts;//用于存储正在put操作的数据
  private long searchTime = 0;
  private long searchCount = 0;//查询总时间
  private long copyTime = 0;
  private long copyCount = 0;
  private DB db;//数据库对象
  private Set<Long> queueSet;

  /**
   * @param capacity max event capacity of queue
   * @throws IOException
   */
  FlumeEventQueue(EventQueueBackingStore backingStore, File inflightTakesFile,
                  File inflightPutsFile, File queueSetDBDir) throws Exception {
    Preconditions.checkArgument(backingStore.getCapacity() > 0,
        "Capacity must be greater than zero");
    Preconditions.checkNotNull(backingStore, "backingStore");
    this.channelNameDescriptor = "[channel=" + backingStore.getName() + "]";
    Preconditions.checkNotNull(inflightTakesFile, "inflightTakesFile");
    Preconditions.checkNotNull(inflightPutsFile, "inflightPutsFile");
    Preconditions.checkNotNull(queueSetDBDir, "queueSetDBDir");
    this.backingStore = backingStore;
    try {
      inflightPuts = new InflightEventWrapper(inflightPutsFile);
      inflightTakes = new InflightEventWrapper(inflightTakesFile);
    } catch (Exception e) {
      LOG.error("Could not read checkpoint.", e);
      throw e;
    }
    if (queueSetDBDir.isDirectory()) {
      FileUtils.deleteDirectory(queueSetDBDir);
    } else if (queueSetDBDir.isFile() && !queueSetDBDir.delete()) {
      throw new IOException("QueueSetDir " + queueSetDBDir + " is a file and"
          + " could not be deleted");
    }
    if (!queueSetDBDir.mkdirs()) {
      throw new IllegalStateException("Could not create QueueSet Dir "
          + queueSetDBDir);
    }
    File dbFile = new File(queueSetDBDir, "db");
    db = DBMaker.newFileDB(dbFile)
        .closeOnJvmShutdown()
        .transactionDisable()
        .syncOnCommitDisable()
        .deleteFilesAfterClose()
        .cacheDisable()
        .mmapFileEnableIfSupported()
        .make();
    queueSet =
        db.createHashSet("QueueSet " + " - " + backingStore.getName()).make();
    long start = System.currentTimeMillis();
    for (int i = 0; i < backingStore.getSize(); i++) {
      queueSet.add(get(i));
    }
    LOG.info("QueueSet population inserting " + backingStore.getSize()
        + " took " + (System.currentTimeMillis() - start));
  }

  //反序列化
  SetMultimap<Long, Long> deserializeInflightPuts()
      throws IOException, BadCheckpointException {
    return inflightPuts.deserialize();
  }

  SetMultimap<Long, Long> deserializeInflightTakes()
      throws IOException, BadCheckpointException {
    return inflightTakes.deserialize();
  }

  synchronized long getLogWriteOrderID() {
    return backingStore.getLogWriteOrderID();
  }

  synchronized boolean checkpoint(boolean force) throws Exception {
    if (!backingStore.syncRequired()
        && !inflightTakes.syncRequired()
        && !force) { //No need to check inflight puts, since that would
      //cause elements.syncRequired() to return true.
      LOG.debug("Checkpoint not required");
      return false;
    }
    backingStore.beginCheckpoint();
    inflightPuts.serializeAndWrite();
    inflightTakes.serializeAndWrite();
    backingStore.checkpoint();
    return true;
  }

  /**
   * Retrieve and remove the head of the queue.
   *
   * @return FlumeEventPointer or null if queue is empty
   */
  synchronized FlumeEventPointer removeHead(long transactionID) {
    if (backingStore.getSize() == 0) {
      return null;
    }

    long value = remove(0, transactionID);
    Preconditions.checkState(value != EMPTY, "Empty value "
        + channelNameDescriptor);

    FlumeEventPointer ptr = FlumeEventPointer.fromLong(value);
    backingStore.decrementFileID(ptr.getFileID());
    return ptr;
  }

  /**
   * Add a FlumeEventPointer to the head of the queue.
   * Called during rollbacks.
   *
   * @param FlumeEventPointer to be added
   * @return true if space was available and pointer was
   * added to the queue
   */
  synchronized boolean addHead(FlumeEventPointer e) {
    //Called only during rollback, so should not consider inflight takes' size,
    //because normal puts through addTail method already account for these
    //events since they are in the inflight takes. So puts will not happen
    //in such a way that these takes cannot go back in. If this if returns true,
    //there is a buuuuuuuug!
    if (backingStore.getSize() == backingStore.getCapacity()) {
      LOG.error("Could not reinsert to queue, events which were taken but "
          + "not committed. Please report this issue.");
      return false;
    }

    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    backingStore.incrementFileID(e.getFileID());

    add(0, value);
    return true;
  }


  /**
   * Add a FlumeEventPointer to the tail of the queue.
   *
   * @param FlumeEventPointer to be added
   * @return true if space was available and pointer
   * was added to the queue
   */
  synchronized boolean addTail(FlumeEventPointer e) {
    if (getSize() == backingStore.getCapacity()) {
      return false;
    }

    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    backingStore.incrementFileID(e.getFileID());

    add(backingStore.getSize(), value);
    return true;
  }

  /**
   * Must be called when a put happens to the log. This ensures that put commits
   * after checkpoints will retrieve all events committed in that txn.
   * 说明已经元素已经put到log中了,但是没有被commit
   * @param e
   * @param transactionID   该提交属于哪个事务提交的
   */
  synchronized void addWithoutCommit(FlumeEventPointer e, long transactionID) {
    inflightPuts.addEvent(transactionID, e.toLong());
  }

  /**
   * Remove FlumeEventPointer from queue, will
   * only be used when recovering from a crash. It is not
   * legal to call this method after replayComplete has been
   * called.
   *
   * @param FlumeEventPointer to be removed
   * @return true if the FlumeEventPointer was found
   * and removed
   */
  // remove() overloads should not be split, according to checkstyle.
  // CHECKSTYLE:OFF
  synchronized boolean remove(FlumeEventPointer e) {
    long value = e.toLong();
    Preconditions.checkArgument(value != EMPTY);
    if (queueSet == null) {
      throw new IllegalStateException("QueueSet is null, thus replayComplete"
          + " has been called which is illegal");
    }
    if (!queueSet.contains(value)) {
      return false;
    }
    searchCount++;
    long start = System.currentTimeMillis();
    for (int i = 0; i < backingStore.getSize(); i++) {
      if (get(i) == value) {
        remove(i, 0);
        FlumeEventPointer ptr = FlumeEventPointer.fromLong(value);
        backingStore.decrementFileID(ptr.getFileID());
        searchTime += System.currentTimeMillis() - start;
        return true;
      }
    }
    searchTime += System.currentTimeMillis() - start;
    return false;
  }
  // CHECKSTYLE:ON

  /**
   * @return a copy of the set of fileIDs which are currently on the queue
   * will be normally be used when deciding which data files can
   * be deleted
   */
  synchronized SortedSet<Integer> getFileIDs() {
    //Java implements clone pretty well. The main place this is used
    //in checkpointing and deleting old files, so best
    //to use a sorted set implementation.
    SortedSet<Integer> fileIDs =
        new TreeSet<Integer>(backingStore.getReferenceCounts());
    fileIDs.addAll(inflightPuts.getFileIDs());
    fileIDs.addAll(inflightTakes.getFileIDs());
    return fileIDs;
  }

  protected long get(int index) {
    if (index < 0 || index > backingStore.getSize() - 1) {
      throw new IndexOutOfBoundsException(String.valueOf(index)
          + channelNameDescriptor);
    }
    return backingStore.get(index);
  }

  private void set(int index, long value) {
    if (index < 0 || index > backingStore.getSize() - 1) {
      throw new IndexOutOfBoundsException(String.valueOf(index)
          + channelNameDescriptor);
    }
    backingStore.put(index, value);
  }

  protected boolean add(int index, long value) {
    if (index < 0 || index > backingStore.getSize()) {
      throw new IndexOutOfBoundsException(String.valueOf(index)
          + channelNameDescriptor);
    }

    if (backingStore.getSize() == backingStore.getCapacity()) {
      return false;
    }

    backingStore.setSize(backingStore.getSize() + 1);

    if (index <= backingStore.getSize() / 2) {
      // Shift left
      backingStore.setHead(backingStore.getHead() - 1);
      if (backingStore.getHead() < 0) {
        backingStore.setHead(backingStore.getCapacity() - 1);
      }
      for (int i = 0; i < index; i++) {
        set(i, get(i + 1));
      }
    } else {
      // Sift right
      for (int i = backingStore.getSize() - 1; i > index; i--) {
        set(i, get(i - 1));
      }
    }
    set(index, value);
    if (queueSet != null) {
      queueSet.add(value);
    }
    return true;
  }

  /**
   * Must be called when a transaction is being committed or rolled back.
   *
   * @param transactionID
   */
  synchronized void completeTransaction(long transactionID) {
    if (!inflightPuts.completeTransaction(transactionID)) {
      inflightTakes.completeTransaction(transactionID);
    }
  }

  protected synchronized long remove(int index, long transactionID) {
    if (index < 0 || index > backingStore.getSize() - 1) {
      throw new IndexOutOfBoundsException("index = " + index
          + ", queueSize " + backingStore.getSize() + " " + channelNameDescriptor);
    }
    copyCount++;
    long start = System.currentTimeMillis();
    long value = get(index);
    if (queueSet != null) {
      queueSet.remove(value);
    }
    //if txn id = 0, we are recovering from a crash.
    if (transactionID != 0) {
      inflightTakes.addEvent(transactionID, value);
    }
    if (index > backingStore.getSize() / 2) {
      // Move tail part to left
      for (int i = index; i < backingStore.getSize() - 1; i++) {
        long rightValue = get(i + 1);
        set(i, rightValue);
      }
      set(backingStore.getSize() - 1, EMPTY);
    } else {
      // Move head part to right
      for (int i = index - 1; i >= 0; i--) {
        long leftValue = get(i);
        set(i + 1, leftValue);
      }
      set(0, EMPTY);
      backingStore.setHead(backingStore.getHead() + 1);
      if (backingStore.getHead() == backingStore.getCapacity()) {
        backingStore.setHead(0);
      }
    }
    backingStore.setSize(backingStore.getSize() - 1);
    copyTime += System.currentTimeMillis() - start;
    return value;
  }

  protected synchronized int getSize() {
    return backingStore.getSize() + inflightTakes.getSize();
  }

  /**
   * @return max capacity of the queue
   */
  public int getCapacity() {
    return backingStore.getCapacity();
  }

  synchronized void close() throws IOException {
    try {
      if (db != null) {
        db.close();
      }
    } catch (Exception ex) {
      LOG.warn("Error closing db", ex);
    }
    try {
      backingStore.close();
      inflightPuts.close();
      inflightTakes.close();
    } catch (IOException e) {
      LOG.warn("Error closing backing store", e);
    }
  }

  /**
   * Called when ReplayHandler has completed and thus remove(FlumeEventPointer)
   * will no longer be called.
   */
  synchronized void replayComplete() {
    String msg = "Search Count = " + searchCount + ", Search Time = " +
        searchTime + ", Copy Count = " + copyCount + ", Copy Time = " +
        copyTime;
    LOG.info(msg);
    if (db != null) {
      db.close();
    }
    queueSet = null;
    db = null;
  }

  @VisibleForTesting
  long getSearchCount() {
    return searchCount;
  }

  @VisibleForTesting
  long getCopyCount() {
    return copyCount;
  }

  /**
   * A representation of in flight events which have not yet been committed.
   * None of the methods are thread safe, and should be called from thread
   * safe methods only.
   * 代表正在处理中的事件,即此时事件尚未被提交
   * 该类的方法都不是线程安全的,因此要确保线程安全的时候去调用
   *
   *
   * 数据序列化内容格式:
   * 16个字节的校验和
   * long:事务ID
   * long:事务ID下有多少条数据
   * n个long:即每一个具体的数据--每一个long表示文件的ID以及offset
   * 循环每一个事务ID。。。
   *
   */
  class InflightEventWrapper {

    //key是事务ID,value一个Long类型的集合,每一个long表示事件所在的文件ID以及offset偏移量组成的long
    private SetMultimap<Long, Long> inflightEvents = HashMultimap.create();
    // Both these are volatile for safe publication, they are never accessed by
    // more than 1 thread at a time.
    private volatile RandomAccessFile file;
    private volatile java.nio.channels.FileChannel fileChannel;
    private final MessageDigest digest;//MD5算法
    private final File inflightEventsFile;
    private volatile boolean syncRequired = false;//true表示需要去同步数据了,数据已经脏了

      //key是事务ID,value是一个Long类型的集合,每一个long表示该事务所在的文件ID
    private SetMultimap<Long, Integer> inflightFileIDs = HashMultimap.create();

    public InflightEventWrapper(File inflightEventsFile) throws Exception {
      if (!inflightEventsFile.exists()) {
        Preconditions.checkState(inflightEventsFile.createNewFile(), "Could not"
            + "create inflight events file: "
            + inflightEventsFile.getCanonicalPath());
      }
      this.inflightEventsFile = inflightEventsFile;
      file = new RandomAccessFile(inflightEventsFile, "rw");
      fileChannel = file.getChannel();
      digest = MessageDigest.getInstance("MD5");
    }

    /**
     * Complete the transaction, and remove all events from inflight list.
     * 说明一个事务已经完成,则清理处理中的数据
     * @param transactionID
     */
    public boolean completeTransaction(Long transactionID) {
      if (!inflightEvents.containsKey(transactionID)) {
        return false;
      }
      inflightEvents.removeAll(transactionID);
      inflightFileIDs.removeAll(transactionID);
      syncRequired = true;
      return true;
    }

    /**
     * Add an event pointer to the inflights list.
     * 添加一个事件
     * @param transactionID
     * @param pointer
     */
    public void addEvent(Long transactionID, Long pointer) {
      inflightEvents.put(transactionID, pointer);
      inflightFileIDs.put(transactionID,
          FlumeEventPointer.fromLong(pointer).getFileID());
      syncRequired = true;
    }

    /**
     * Serialize the set of in flights into a byte longBuffer.
     * 将在处理中的数据组装到longBuffer中,即都是long类型的
     * @return Returns the checksum of the buffer that is being
     * asynchronously written to disk.
     */
    public void serializeAndWrite() throws Exception {
      Collection<Long> values = inflightEvents.values();//处理中的数据集合
      if (!fileChannel.isOpen()) {
        file = new RandomAccessFile(inflightEventsFile, "rw");
        fileChannel = file.getChannel();
      }
      if (values.isEmpty()) {
        file.setLength(0L);
      }
      //What is written out?
      //Checksum - 16 bytes
      //and then each key-value pair from the map:
      //transactionid numberofeventsforthistxn listofeventpointers

      try {
        //期望的文件大小,因为一个long是由文件ID和offset组成的,因此一个long可以产生2个int
        int expectedFileSize = (((inflightEvents.keySet().size() * 2) //for transactionIDs and
                                                                      //events per txn ID //此时表示有多少个事务,以及每一个事务有多少个元素,因此有多少个事务,就需要2倍的数据字节存储,比如有10个事务,那么就需要记录10个事务ID以及每一个事务下有多少个元素
            + values.size()) * 8) //Event pointers,values.size()表示有多少long值,即具体的long值....*8表示每一个数据都是用8个byte表示,因此就是总字节数
            + 16; //Checksum 校验和
        //There is no real need of filling the channel with 0s, since we
        //will write the exact number of bytes as expected file size.
        file.setLength(expectedFileSize);
        Preconditions.checkState(file.length() == expectedFileSize,
            "Expected File size of inflight events file does not match the "
                + "current file size. Checkpoint is incomplete.");
        file.seek(0);
        final ByteBuffer buffer = ByteBuffer.allocate(expectedFileSize);//分配内存
        LongBuffer longBuffer = buffer.asLongBuffer();
        for (Long txnID : inflightEvents.keySet()) {//循环事务ID
          Set<Long> pointers = inflightEvents.get(txnID);//返回该事务对应的所有value集合
          longBuffer.put(txnID);//存储事务ID
          longBuffer.put((long) pointers.size());//事务下的数据size
          LOG.debug("Number of events inserted into "
              + "inflights file: " + String.valueOf(pointers.size())
              + " file: " + inflightEventsFile.getCanonicalPath());
          long[] written = ArrayUtils.toPrimitive(
              pointers.toArray(new Long[0]));
          longBuffer.put(written);//写入该事务下所有的value
        }
        byte[] checksum = digest.digest(buffer.array());
        file.write(checksum);//先写入校验和
        buffer.position(0);
        fileChannel.write(buffer);
        fileChannel.force(true);
        syncRequired = false;//同步完成
      } catch (IOException ex) {
        LOG.error("Error while writing checkpoint to disk.", ex);
        throw ex;
      }
    }

    /**
     * Read the inflights file and return a
     * {@link com.google.common.collect.SetMultimap}
     * of transactionIDs to events that were inflight.
     *
     * @return - map of inflight events per txnID.
     * 反序列化
     */
    public SetMultimap<Long, Long> deserialize()
        throws IOException, BadCheckpointException {
      SetMultimap<Long, Long> inflights = HashMultimap.create();
      if (!fileChannel.isOpen()) {
        file = new RandomAccessFile(inflightEventsFile, "rw");
        fileChannel = file.getChannel();
      }
      if (file.length() == 0) {
        return inflights;
      }
      file.seek(0);
      byte[] checksum = new byte[16];
      file.read(checksum);//读取校验和--前16个字节
      ByteBuffer buffer = ByteBuffer.allocate(
          (int) (file.length() - file.getFilePointer()));
      fileChannel.read(buffer);//读取数据内容
      byte[] fileChecksum = digest.digest(buffer.array());//查看校验和是否正确
      if (!Arrays.equals(checksum, fileChecksum)) {
        throw new BadCheckpointException("Checksum of inflights file differs"
            + " from the checksum expected.");
      }
      buffer.position(0);
      LongBuffer longBuffer = buffer.asLongBuffer();
      try {
        while (true) {
          long txnID = longBuffer.get();//事务ID
          int numEvents = (int) (longBuffer.get());//事务ID有多少条数据
          for (int i = 0; i < numEvents; i++) {//还原每一条数据
            long val = longBuffer.get();
            inflights.put(txnID, val);
          }
        }
      } catch (BufferUnderflowException ex) {
        LOG.debug("Reached end of inflights buffer. Long buffer position ="
            + String.valueOf(longBuffer.position()));
      }
      return inflights;
    }

    public int getSize() {
      return inflightEvents.size();
    }

    public boolean syncRequired() {
      return syncRequired;
    }

    public Collection<Integer> getFileIDs() {
      return inflightFileIDs.values();
    }

    //Needed for testing.
    public Collection<Long> getInFlightPointers() {
      return inflightEvents.values();
    }

    public void close() throws IOException {
      file.close();
    }
  }
}
