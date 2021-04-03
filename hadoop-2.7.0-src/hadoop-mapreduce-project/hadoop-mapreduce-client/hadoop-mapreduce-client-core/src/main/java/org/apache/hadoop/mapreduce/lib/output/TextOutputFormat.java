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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.*;

/** An {@link OutputFormat} that writes plain text files. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TextOutputFormat<K, V> extends FileOutputFormat<K, V> {
  public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
  protected static class LineRecordWriter<K, V>
    extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {
        Text to = (Text) o;
        out.write(to.getBytes(), 0, to.getLength());
      } else {
        out.write(o.toString().getBytes(utf8));
      }
    }

    public synchronized void write(K key, V value)
      throws IOException {

      // 判断key是否为空
      boolean nullKey = key == null || key instanceof NullWritable;
      // 判断value是否空
      boolean nullValue = value == null || value instanceof NullWritable;
      // key value都是null  直接返回
      if (nullKey && nullValue) {
        return;
      }
      // key不为空  将key写出去
      if (!nullKey) {
        writeObject(key);
      }
      /**
       *  || 或  有短路的能力  如果第一个为true，就不会在看第二个表达式了
       *  假如key为null  那么nullKey就是true，不用再看nullValue的值：
       *                  (nullKey || nullValue) 就为true  !(nullKey || nullValue) 为false，
       *                  不会进if
       *  假如key不为null  那么nullKey就是false，继续看nullValue的值：
       *                  nullValue 为 true ：
       *                        (nullKey || nullValue) 就为true  !(nullKey || nullValue) 为 false，
       *                        不会进if
       *                  nullValue 为 false ：
       *                        (nullKey || nullValue) 就为false  !(nullKey || nullValue) 为 true，
       *                        进入if
       *  总结：只有key和value都不是空的时候才会进if，
       *  if 就是 写一个key和value之间的分隔符
       */
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      // 如果value不为null，将value写出去
      if (!nullValue) {
        writeObject(value);
      }

      // newline就是换行符
      out.write(newline);
    }

    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
    }
  }

  public RecordWriter<K, V> 
         getRecordWriter(TaskAttemptContext job
                         ) throws IOException, InterruptedException {
    Configuration conf = job.getConfiguration();
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator= conf.get(SEPERATOR, "\t");
    CompressionCodec codec = null;
    String extension = "";
    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = 
        getOutputCompressorClass(job, GzipCodec.class);
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
      extension = codec.getDefaultExtension();
    }
    Path file = getDefaultWorkFile(job, extension);
    FileSystem fs = file.getFileSystem(conf);
    if (!isCompressed) {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    } else {
      FSDataOutputStream fileOut = fs.create(file, false);
      return new LineRecordWriter<K, V>(new DataOutputStream
                                        (codec.createOutputStream(fileOut)),
                                        keyValueSeparator);
    }
  }
}

