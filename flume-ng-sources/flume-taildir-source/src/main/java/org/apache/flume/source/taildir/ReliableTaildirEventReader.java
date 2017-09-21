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

package org.apache.flume.source.taildir;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

//可靠的方式读取文件
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

  private final List<TaildirMatcher> taildirCache;//每一个path匹配对应的TaildirMatcher对象集合
  private final Table<String, String, String> headerTable;

  private TailFile currentFile = null;//当前在读取哪个文件
  private Map<Long, TailFile> tailFiles = Maps.newHashMap();//key是inode,表示唯一的文件,value是该文件如何读取信息的对象
  private long updateTime;
  private boolean addByteOffset;//true表示要返回该行数据第一个字节在文件中的偏移量
  private boolean cachePatternMatching;//匹配的文件集合是否需要缓存
  private boolean committed = true;//可靠的,因此会有一个提交的过程
  private final boolean annotateFileName;//true表示事件会知道这些数据是来自于哪个文件
  private final String fileNameHeader;//用于存储表示文件名的key,方便事件通过该key获取文件名

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(Map<String, String> filePaths,//key是文件组name,value是匹配的文件path路径表达式
      Table<String, String, String> headerTable,
      String positionFilePath,//读取json文件,该文件存储每一个file文件最后读取的position位置
      boolean skipToEnd,//表示从文件的最后开始读取,还是从文件的头开始读取,true表示从文件最后开始读取
      boolean addByteOffset,//true表示要返回该行数据第一个字节在文件中的偏移量
      boolean cachePatternMatching,//匹配的文件集合是否需要缓存
      boolean annotateFileName,//true表示事件会知道这些数据是来自于哪个文件
      String fileNameHeader) throws IOException {//用于存储表示文件名的key,方便事件通过该key获取文件名
    // Sanity checks
    Preconditions.checkNotNull(filePaths);
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}",
          new Object[] { ReliableTaildirEventReader.class.getSimpleName(), filePaths });
    }

    //每一个TaildirMatcher表示可以通过一个文件路径获取匹配的文件集合
    List<TaildirMatcher> taildirCache = Lists.newArrayList();
    for (Entry<String, String> e : filePaths.entrySet()) {//循环每一个文件路径表达式
      taildirCache.add(new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));//去匹配需要的文件
    }
    logger.info("taildirCache: " + taildirCache.toString());
    logger.info("headerTable: " + headerTable.toString());

    this.taildirCache = taildirCache;
    this.headerTable = headerTable;
    this.addByteOffset = addByteOffset;
    this.cachePatternMatching = cachePatternMatching;
    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;
    updateTailFiles(skipToEnd);

    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);//读取json文件,该文件存储每一个file文件最后读取的position位置
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update tailFiles mapping.
   * 读取json文件,该文件存储每一个file文件最后读取的position位置
   */
  public void loadPositionFile(String filePath) {
    Long inode, pos;
    String path;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      jr = new JsonReader(fr);//将文件转换给json对象
      jr.beginArray();
      while (jr.hasNext()) {
        inode = null;
        pos = null;
        path = null;
        jr.beginObject();
        while (jr.hasNext()) {//一行由inode、pos、file组成
          switch (jr.nextName()) {
            case "inode":
              inode = jr.nextLong();
              break;
            case "pos":
              pos = jr.nextLong();
              break;
            case "file":
              path = jr.nextString();
              break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList(inode, pos, path)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "inode: " + inode + ", pos: " + pos + ", path: " + path);
        }
        TailFile tf = tailFiles.get(inode);
        if (tf != null && tf.updatePos(path, inode, pos)) {//更新最后读取的pos位置
          tailFiles.put(inode, tf);
        } else {
          logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (IOException e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<Long, TailFile> getTailFiles() {
    return tailFiles;
  }

  public void setCurrentFile(TailFile currentFile) {
    this.currentFile = currentFile;
  }

    //读取一行数据
  @Override
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

    //读取多行数据
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

    //读取某一个文件的多行数据
  @VisibleForTesting
  public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }

    //读取多行记录
  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.updateFilePos(lastPos);
    }
    List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);//读取若干行数据
    if (events.isEmpty()) {
      return events;
    }

    Map<String, String> headers = currentFile.getHeaders();//头文件信息
    if (annotateFileName || (headers != null && !headers.isEmpty())) {
      for (Event event : events) {//循环每一个事件
        if (headers != null && !headers.isEmpty()) {
          event.getHeaders().putAll(headers);//将文件的header信息添加到每一个事件里面
        }
        if (annotateFileName) {
          event.getHeaders().put(fileNameHeader, currentFile.getPath());//true表示事件会知道这些数据是来自于哪个文件
        }
      }
    }
    committed = false;
    return events;
  }

  @Override
  public void close() throws IOException {
    for (TailFile tf : tailFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getLineReadPos();
      currentFile.setPos(pos);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }

  /**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   * @param skipToEnd 表示从文件的最后开始读取,还是从文件的头开始读取,true表示从文件最后开始读取
   * 当有新文件的时候,会将新文件也加入到读取集合中
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
    updateTime = System.currentTimeMillis();
    List<Long> updatedInodes = Lists.newArrayList();

    for (TaildirMatcher taildir : taildirCache) {//循环每一个路径匹配对象
      Map<String, String> headers = headerTable.row(taildir.getFileGroup());

      for (File f : taildir.getMatchingFiles()) {//返回匹配的文件集合
        long inode = getInode(f);//文件的inode
        TailFile tf = tailFiles.get(inode);//找到要读取的文件对象
        if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {//说明文件没有读取过
          long startPos = skipToEnd ? f.length() : 0;//设置从头开始读取还是从尾巴开始读取
          tf = openFile(f, headers, inode, startPos);//组装成文件读取对象
        } else {
          boolean updated = tf.getLastUpdated() < f.lastModified();//说明文件有更新
          if (updated) {
            if (tf.getRaf() == null) {
              tf = openFile(f, headers, inode, tf.getPos());
            }
            if (f.length() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                  + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
              tf.updatePos(tf.getPath(), inode, 0);
            }
          }
          tf.setNeedTail(updated);
        }
        tailFiles.put(inode, tf);
        updatedInodes.add(inode);
      }
    }
    return updatedInodes;
  }

  public List<Long> updateTailFiles() throws IOException {
    return updateTailFiles(false);
  }


    //获取一个文件的inode
  private long getInode(File file) throws IOException {
    long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
    return inode;
  }

    //打开一个文件
  private TailFile openFile(File file, Map<String, String> headers, long inode, long pos) {
    try {
      logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
      return new TailFile(file, headers, inode, pos);
    } catch (IOException e) {
      throw new FlumeException("Failed opening file: " + file, e);
    }
  }

  /**
   * Special builder class for ReliableTaildirEventReader
   */
  public static class Builder {
    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;
    private boolean cachePatternMatching;
    private Boolean annotateFileName =
            TaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
    private String fileNameHeader =
            TaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

    public Builder filePaths(Map<String, String> filePaths) {
      this.filePaths = filePaths;
      return this;
    }

    public Builder headerTable(Table<String, String, String> headerTable) {
      this.headerTable = headerTable;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder cachePatternMatching(boolean cachePatternMatching) {
      this.cachePatternMatching = cachePatternMatching;
      return this;
    }

    public Builder annotateFileName(boolean annotateFileName) {
      this.annotateFileName = annotateFileName;
      return this;
    }

    public Builder fileNameHeader(String fileNameHeader) {
      this.fileNameHeader = fileNameHeader;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd,
                                            addByteOffset, cachePatternMatching,
                                            annotateFileName, fileNameHeader);
    }
  }

}
