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

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

//如何一行一行读取一个文件
public class TailFile {
  private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

    //回车换行
  private static final byte BYTE_NL = (byte) 10;//换行
  private static final byte BYTE_CR = (byte) 13;//回车

  private static final int BUFFER_SIZE = 8192;//缓冲区大小
  private static final int NEED_READING = -1;

  private RandomAccessFile raf;//文件本身
  private final String path;//文件路径
  private final long inode;//文件的inode值
  private long pos;//文件的pos位置
  private long lastUpdated;//最后更新时间
  private boolean needTail;//暂时觉得是是否tail方式执行该文件,即没有文件内容的时候也要等待
  private final Map<String, String> headers;//用于存储一个事件的很多key=value特殊信息
  private byte[] buffer;//用于存储预先读取的文件内容
  private byte[] oldBuffer;//用于存储一行内的数据
  private int bufferPos;//缓冲区的pos位置
  private long lineReadPos;//设置回车换行的位置

  public TailFile(File file, Map<String, String> headers, long inode, long pos)
      throws IOException {
    this.raf = new RandomAccessFile(file, "r");
    if (pos > 0) {
      raf.seek(pos);
      lineReadPos = pos;
    }
    this.path = file.getAbsolutePath();
    this.inode = inode;
    this.pos = pos;
    this.lastUpdated = 0L;
    this.needTail = true;
    this.headers = headers;
    this.oldBuffer = new byte[0];
    this.bufferPos = NEED_READING;
  }

  public RandomAccessFile getRaf() {
    return raf;
  }

  public String getPath() {
    return path;
  }

  public long getInode() {
    return inode;
  }

  public long getPos() {
    return pos;
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public boolean needTail() {
    return needTail;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public long getLineReadPos() {
    return lineReadPos;
  }

  public void setPos(long pos) {
    this.pos = pos;
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  public void setNeedTail(boolean needTail) {
    this.needTail = needTail;
  }

  public void setLineReadPos(long lineReadPos) {
    this.lineReadPos = lineReadPos;
  }

  public boolean updatePos(String path, long inode, long pos) throws IOException {
    if (this.inode == inode && this.path.equals(path)) {//确保文件是该文件
      setPos(pos);
      updateFilePos(pos);
      logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
      return true;
    }
    return false;
  }

  //重新设置文件位置,即还原读取该行数据之前的位置
  public void updateFilePos(long pos) throws IOException {
    raf.seek(pos);
    lineReadPos = pos;
    //重置缓冲区
    bufferPos = NEED_READING;
    oldBuffer = new byte[0];
  }

    /**
     * 读取若干个事件,返回事件集合
     * @param numEvents 要读取多少个事件
     * @param backoffWithoutNL true表示如果没有换行字符存在,则抛弃该数据
     * @param addByteOffset true表示要返回该行数据第一个字节在文件中的偏移量
     * @return
     * @throws IOException
     */
  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL,
      boolean addByteOffset) throws IOException {
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent(backoffWithoutNL, addByteOffset);
      if (event == null) {//说明此时没有读取到一行数据
        break;
      }
      events.add(event);
    }
    return events;
  }

    /**
     * 读取一个事件,即读取一行数据
     * @param backoffWithoutNL true表示如果没有换行字符存在,则抛弃该数据
     * @param addByteOffset true表示要返回该行数据第一个字节在文件中的偏移量
     * @return
     * @throws IOException
     */
  private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
    Long posTmp = getLineReadPos();//回车换行的位置
    LineResult line = readLine();//读取一行数据
    if (line == null) {
      return null;
    }
    if (backoffWithoutNL && !line.lineSepInclude) {
      logger.info("Backing off in file without newline: "
          + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());//说明此时是文件结尾,目前读取到的文件结尾,此时并没有回车换行出现,因此可能未来还会继续写入数据,因此暂时不读去数据
      updateFilePos(posTmp);//重新设置文件位置,即还原读取该行数据之前的位置
      return null;
    }
    Event event = EventBuilder.withBody(line.line);//一行内容组装成一个事件
    if (addByteOffset == true) {
      event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());//设置该行内容在文件中的偏移量
    }
    return event;
  }

    //读取文件,将内容读取到缓冲区里
  private void readFile() throws IOException {
    if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {//说明缓冲区大
      buffer = new byte[(int) (raf.length() - raf.getFilePointer())];//精确到读取多少个字节
    } else {
      buffer = new byte[BUFFER_SIZE];//读取一个缓冲区默认大小
    }
    raf.read(buffer, 0, buffer.length);//读取文件
    bufferPos = 0;
  }

    //将a和b的数据内容merge
  private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
                                  byte[] b, int startIdxB, int lenB) {
    byte[] c = new byte[lenA + lenB];
    System.arraycopy(a, startIdxA, c, 0, lenA);
    System.arraycopy(b, startIdxB, c, lenA, lenB);
    return c;
  }

  public LineResult readLine() throws IOException {
    LineResult lineResult = null;
    while (true) {
      if (bufferPos == NEED_READING) {//说明没有缓冲区数据了,因此要读取数据
        if (raf.getFilePointer() < raf.length()) {
          readFile();//读取数据
        } else {//说明文件已经到结尾了,将以前的old缓冲区内容返回即可
          if (oldBuffer.length > 0) {
            lineResult = new LineResult(false, oldBuffer);
            oldBuffer = new byte[0];
            setLineReadPos(lineReadPos + lineResult.line.length);
          }
          break;
        }
      }
      for (int i = bufferPos; i < buffer.length; i++) {//不断从缓冲区读取数据
        if (buffer[i] == BYTE_NL) {//说明是换行
          int oldLen = oldBuffer.length;
          // Don't copy last byte(NEW_LINE)不需要复制回车这个字节
          int lineLen = i - bufferPos;//一行的内容所有字节数
          // For windows, check for CR
          if (i > 0 && buffer[i - 1] == BYTE_CR) {//是否前一个是回车,是则取消该位置,也不能被读
            lineLen -= 1;
          } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
            oldLen -= 1;
          }
          lineResult = new LineResult(true,
              concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));//合并结果
          setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));//设置回车换行的位置
          oldBuffer = new byte[0];
          if (i + 1 < buffer.length) {//设置下一次缓冲区读取的位置
            bufferPos = i + 1;
          } else {
            bufferPos = NEED_READING;
          }
          break;
        }
      }
      if (lineResult != null) {//说明没有读取到一行数据
        break;
      }
      // NEW_LINE not showed up at the end of the buffer
      oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length,
                                   buffer, bufferPos, buffer.length - bufferPos);//重新设置old内容
      bufferPos = NEED_READING;//缓冲区结束
    }
    return lineResult;
  }

    //关闭该文件
  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path + ", inode: " + inode, e);
    }
  }

    //表示一行数据
  private class LineResult {
    final boolean lineSepInclude;//是否包含了换行字符
    final byte[] line;//一行数据的内容

    public LineResult(boolean lineSepInclude, byte[] line) {
      super();
      this.lineSepInclude = lineSepInclude;
      this.line = line;
    }
  }
}
