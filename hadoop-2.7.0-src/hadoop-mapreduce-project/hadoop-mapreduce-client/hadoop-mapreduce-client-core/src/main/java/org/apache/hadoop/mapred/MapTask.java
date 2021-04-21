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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;

/** A Map task. */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MapTask extends Task {
  /**
   * The size of each record in the index file for the map-outputs.
   */
  public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

  private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  private final static int APPROX_HEADER_LENGTH = 150;

  private static final Log LOG = LogFactory.getLog(MapTask.class.getName());

  private Progress mapPhase;
  private Progress sortPhase;
  
  {   // set phase for this task
    setPhase(TaskStatus.Phase.MAP); 
    getProgress().setStatus("map");
  }

  public MapTask() {
    super();
  }

  public MapTask(String jobFile, TaskAttemptID taskId, 
                 int partition, TaskSplitIndex splitIndex,
                 int numSlotsRequired) {
    super(jobFile, taskId, partition, numSlotsRequired);
    this.splitMetaInfo = splitIndex;
  }

  @Override
  public boolean isMapTask() {
    return true;
  }

  @Override
  public void localizeConfiguration(JobConf conf)
      throws IOException {
    super.localizeConfiguration(conf);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    if (isMapOrReduce()) {
      splitMetaInfo.write(out);
      splitMetaInfo = null;
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    if (isMapOrReduce()) {
      splitMetaInfo.readFields(in);
    }
  }

  /**
   * This class wraps the user's record reader to update the counters and progress
   * as records are read.
   * @param <K>
   * @param <V>
   */
  class TrackedRecordReader<K, V> 
      implements RecordReader<K,V> {
    private RecordReader<K,V> rawIn;
    private Counters.Counter fileInputByteCounter;
    private Counters.Counter inputRecordCounter;
    private TaskReporter reporter;
    private long bytesInPrev = -1;
    private long bytesInCurr = -1;
    private final List<Statistics> fsStats;
    
    TrackedRecordReader(TaskReporter reporter, JobConf job) 
      throws IOException{
      inputRecordCounter = reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
      fileInputByteCounter = reporter.getCounter(FileInputFormatCounter.BYTES_READ);
      this.reporter = reporter;
      
      List<Statistics> matchedStats = null;
      if (this.reporter.getInputSplit() instanceof FileSplit) {
        matchedStats = getFsStatistics(((FileSplit) this.reporter
            .getInputSplit()).getPath(), job);
      }
      fsStats = matchedStats;

      bytesInPrev = getInputBytes(fsStats);
      rawIn = job.getInputFormat().getRecordReader(reporter.getInputSplit(),
          job, reporter);
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    public K createKey() {
      return rawIn.createKey();
    }
      
    public V createValue() {
      return rawIn.createValue();
    }
     
    public synchronized boolean next(K key, V value)
    throws IOException {
      boolean ret = moveToNext(key, value);
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected void incrCounters() {
      inputRecordCounter.increment(1);
    }
     
    protected synchronized boolean moveToNext(K key, V value)
      throws IOException {
      bytesInPrev = getInputBytes(fsStats);
      boolean ret = rawIn.next(key, value);
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      reporter.setProgress(getProgress());
      return ret;
    }
    
    public long getPos() throws IOException { return rawIn.getPos(); }

    public void close() throws IOException {
      bytesInPrev = getInputBytes(fsStats);
      rawIn.close();
      bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    public float getProgress() throws IOException {
      return rawIn.getProgress();
    }
    TaskReporter getTaskReporter() {
      return reporter;
    }

    private long getInputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesRead = 0;
      for (Statistics stat: stats) {
        bytesRead = bytesRead + stat.getBytesRead();
      }
      return bytesRead;
    }
  }

  /**
   * This class skips the records based on the failed ranges from previous 
   * attempts.
   */
  class SkippingRecordReader<K, V> extends TrackedRecordReader<K,V> {
    private SkipRangeIterator skipIt;
    private SequenceFile.Writer skipWriter;
    private boolean toWriteSkipRecs;
    private TaskUmbilicalProtocol umbilical;
    private Counters.Counter skipRecCounter;
    private long recIndex = -1;
    
    SkippingRecordReader(TaskUmbilicalProtocol umbilical,
                         TaskReporter reporter, JobConf job) throws IOException{
      super(reporter, job);
      this.umbilical = umbilical;
      this.skipRecCounter = reporter.getCounter(TaskCounter.MAP_SKIPPED_RECORDS);
      this.toWriteSkipRecs = toWriteSkipRecs() &&  
        SkipBadRecords.getSkipOutputPath(conf)!=null;
      skipIt = getSkipRanges().skipRangeIterator();
    }
    
    public synchronized boolean next(K key, V value)
    throws IOException {
      if(!skipIt.hasNext()) {
        LOG.warn("Further records got skipped.");
        return false;
      }
      boolean ret = moveToNext(key, value);
      long nextRecIndex = skipIt.next();
      long skip = 0;
      while(recIndex<nextRecIndex && ret) {
        if(toWriteSkipRecs) {
          writeSkippedRec(key, value);
        }
      	ret = moveToNext(key, value);
        skip++;
      }
      //close the skip writer once all the ranges are skipped
      if(skip>0 && skipIt.skippedAllRanges() && skipWriter!=null) {
        skipWriter.close();
      }
      skipRecCounter.increment(skip);
      reportNextRecordRange(umbilical, recIndex);
      if (ret) {
        incrCounters();
      }
      return ret;
    }
    
    protected synchronized boolean moveToNext(K key, V value)
    throws IOException {
	    recIndex++;
      return super.moveToNext(key, value);
    }
    
    @SuppressWarnings("unchecked")
    private void writeSkippedRec(K key, V value) throws IOException{
      if(skipWriter==null) {
        Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
        Path skipFile = new Path(skipDir, getTaskID().toString());
        skipWriter = 
          SequenceFile.createWriter(
              skipFile.getFileSystem(conf), conf, skipFile,
              (Class<K>) createKey().getClass(),
              (Class<V>) createValue().getClass(), 
              CompressionType.BLOCK, getTaskReporter());
      }
      skipWriter.append(key, value);
    }
  }

  @Override
  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, ClassNotFoundException, InterruptedException {
    this.umbilical = umbilical;

    if (isMapTask()) {
      // If there are no reducers then there won't be any sort. Hence the map 
      // phase will govern the entire attempt's progress.
      if (conf.getNumReduceTasks() == 0) {
        mapPhase = getProgress().addPhase("map", 1.0f);
      } else {
        // If there are reducers then the entire attempt's progress will be 
        // split between the map phase (67%) and the sort phase (33%).
        mapPhase = getProgress().addPhase("map", 0.667f);
        sortPhase  = getProgress().addPhase("sort", 0.333f);
      }
    }
    TaskReporter reporter = startReporter(umbilical);
 
    boolean useNewApi = job.getUseNewMapper();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    if (useNewApi) {
      /**
       *
       */
      runNewMapper(job, splitMetaInfo, umbilical, reporter);
    } else {
      runOldMapper(job, splitMetaInfo, umbilical, reporter);
    }
    done(umbilical, reporter);
  }

  public Progress getSortPhase() {
    return sortPhase;
  }

 @SuppressWarnings("unchecked")
 private <T> T getSplitDetails(Path file, long offset) 
  throws IOException {
   FileSystem fs = file.getFileSystem(conf);
   FSDataInputStream inFile = fs.open(file);
   inFile.seek(offset);
   String className = StringInterner.weakIntern(Text.readString(inFile));
   Class<T> cls;
   try {
     cls = (Class<T>) conf.getClassByName(className);
   } catch (ClassNotFoundException ce) {
     IOException wrap = new IOException("Split class " + className + 
                                         " not found");
     wrap.initCause(ce);
     throw wrap;
   }
   SerializationFactory factory = new SerializationFactory(conf);
   Deserializer<T> deserializer = 
     (Deserializer<T>) factory.getDeserializer(cls);
   deserializer.open(inFile);
   T split = deserializer.deserialize(null);
   long pos = inFile.getPos();
   getCounters().findCounter(
       TaskCounter.SPLIT_RAW_BYTES).increment(pos - offset);
   inFile.close();
   return split;
 }
  
  @SuppressWarnings("unchecked")
  private <KEY, VALUE> MapOutputCollector<KEY, VALUE>
          createSortingCollector(JobConf job, TaskReporter reporter)
    throws IOException, ClassNotFoundException {

    MapOutputCollector.Context context =
      new MapOutputCollector.Context(this, job, reporter);

    /***
     *
     */
    Class<?>[] collectorClasses = job.getClasses(
      JobContext.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, MapOutputBuffer.class);
    int remainingCollectors = collectorClasses.length;

    Exception lastException = null;
    for (Class clazz : collectorClasses) {
      try {
        if (!MapOutputCollector.class.isAssignableFrom(clazz)) {
          throw new IOException("Invalid output collector class: " + clazz.getName() +
            " (does not implement MapOutputCollector)");
        }

        /**
         * MapOutputCollector的子类就是MapOutputBuffer
         */
        Class<? extends MapOutputCollector> subclazz =
          clazz.asSubclass(MapOutputCollector.class);
        LOG.debug("Trying map output collector class: " + subclazz.getName());

        /**
         * 反射拿到一个MapOutputCollector对象 （就是MapOutputBuffer）
         */
        MapOutputCollector<KEY, VALUE> collector =
          ReflectionUtils.newInstance(subclazz, job);

        /**
         * 初始化 MapOutputBuffer
         * 重要的两件事：初始化环形缓冲区、启动spill线程
         */
        collector.init(context);


        LOG.info("Map output collector class = " + collector.getClass().getName());
        return collector;
      } catch (Exception e) {
        String msg = "Unable to initialize MapOutputCollector " + clazz.getName();
        if (--remainingCollectors > 0) {
          msg += " (" + remainingCollectors + " more collector(s) to try)";
        }
        lastException = e;
        LOG.warn(msg, e);
      }
    }
    throw new IOException("Initialization of all the collectors failed. " +
      "Error in last collector was :" + lastException.getMessage(), lastException);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runOldMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, InterruptedException,
                             ClassNotFoundException {
    InputSplit inputSplit = getSplitDetails(new Path(splitIndex.getSplitLocation()),
           splitIndex.getStartOffset());

    updateJobWithSplit(job, inputSplit);
    reporter.setInputSplit(inputSplit);

    RecordReader<INKEY,INVALUE> in = isSkipping() ? 
        new SkippingRecordReader<INKEY,INVALUE>(umbilical, reporter, job) :
          new TrackedRecordReader<INKEY,INVALUE>(reporter, job);
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());


    int numReduceTasks = conf.getNumReduceTasks();
    LOG.info("numReduceTasks: " + numReduceTasks);
    MapOutputCollector<OUTKEY, OUTVALUE> collector = null;
    if (numReduceTasks > 0) {
      collector = createSortingCollector(job, reporter);
    } else { 
      collector = new DirectMapOutputCollector<OUTKEY, OUTVALUE>();
       MapOutputCollector.Context context =
                           new MapOutputCollector.Context(this, job, reporter);
      collector.init(context);
    }
    MapRunnable<INKEY,INVALUE,OUTKEY,OUTVALUE> runner =
      ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

    try {
      runner.run(in, new OldOutputCollector(collector, conf), reporter);
      mapPhase.complete();
      // start the sort phase only if there are reducers
      if (numReduceTasks > 0) {
        setPhase(TaskStatus.Phase.SORT);
      }
      statusUpdate(umbilical);
      collector.flush();
      
      in.close();
      in = null;
      
      collector.close();
      collector = null;
    } finally {
      closeQuietly(in);
      closeQuietly(collector);
    }
  }

  /**
   * Update the job with details about the file split
   * @param job the job configuration to update
   * @param inputSplit the file split
   */
  private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
    if (inputSplit instanceof FileSplit) {
      FileSplit fileSplit = (FileSplit) inputSplit;
      job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
      job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
      job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
    }
    LOG.info("Processing split: " + inputSplit);
  }

  static class NewTrackingRecordReader<K,V> 
    extends org.apache.hadoop.mapreduce.RecordReader<K,V> {
    private final org.apache.hadoop.mapreduce.RecordReader<K,V> real;
    private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
    private final org.apache.hadoop.mapreduce.Counter fileInputByteCounter;
    private final TaskReporter reporter;
    private final List<Statistics> fsStats;
    
    NewTrackingRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
        org.apache.hadoop.mapreduce.InputFormat<K, V> inputFormat,
        TaskReporter reporter,
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
        throws InterruptedException, IOException {
      this.reporter = reporter;
      this.inputRecordCounter = reporter
          .getCounter(TaskCounter.MAP_INPUT_RECORDS);
      this.fileInputByteCounter = reporter
          .getCounter(FileInputFormatCounter.BYTES_READ);

      List <Statistics> matchedStats = null;
      if (split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
        matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) split)
            .getPath(), taskContext.getConfiguration());
      }
      fsStats = matchedStats;

      long bytesInPrev = getInputBytes(fsStats);

      /**
       *
       */
      this.real = inputFormat.createRecordReader(split, taskContext);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public void close() throws IOException {
      long bytesInPrev = getInputBytes(fsStats);
      real.close();
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public K getCurrentKey() throws IOException, InterruptedException {
      return real.getCurrentKey();
    }

    @Override
    public V getCurrentValue() throws IOException, InterruptedException {
      return real.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return real.getProgress();
    }

    @Override
    public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                           org.apache.hadoop.mapreduce.TaskAttemptContext context
                           ) throws IOException, InterruptedException {
      long bytesInPrev = getInputBytes(fsStats);
      real.initialize(split, context);
      long bytesInCurr = getInputBytes(fsStats);
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      long bytesInPrev = getInputBytes(fsStats);
      /**
       * real  ： RecordReader  =》 LineRecordReader
       */
      boolean result = real.nextKeyValue();

      long bytesInCurr = getInputBytes(fsStats);
      if (result) {
        inputRecordCounter.increment(1);
      }
      fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
      reporter.setProgress(getProgress());
      return result;
    }

    private long getInputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesRead = 0;
      for (Statistics stat: stats) {
        bytesRead = bytesRead + stat.getBytesRead();
      }
      return bytesRead;
    }
  }

  /**
   * Since the mapred and mapreduce Partitioners don't share a common interface
   * (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
   * partitioner lives in Old/NewOutputCollector. Note that, for map-only jobs,
   * the configured partitioner should not be called. It's common for
   * partitioners to compute a result mod numReduces, which causes a div0 error
   */
  private static class OldOutputCollector<K,V> implements OutputCollector<K,V> {
    private final Partitioner<K,V> partitioner;
    private final MapOutputCollector<K,V> collector;
    private final int numPartitions;

    @SuppressWarnings("unchecked")
    OldOutputCollector(MapOutputCollector<K,V> collector, JobConf conf) {
      numPartitions = conf.getNumReduceTasks();
      if (numPartitions > 1) {
        partitioner = (Partitioner<K,V>)
          ReflectionUtils.newInstance(conf.getPartitionerClass(), conf);
      } else {
        partitioner = new Partitioner<K,V>() {
          @Override
          public void configure(JobConf job) { }
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return numPartitions - 1;
          }
        };
      }
      this.collector = collector;
    }

    @Override
    public void collect(K key, V value) throws IOException {
      try {
        collector.collect(key, value,
                          partitioner.getPartition(key, value, numPartitions));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupt exception", ie);
      }
    }
  }

  private class NewDirectOutputCollector<K,V>
  extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {
    private final org.apache.hadoop.mapreduce.RecordWriter out;

    private final TaskReporter reporter;

    private final Counters.Counter mapOutputRecordCounter;
    private final Counters.Counter fileOutputByteCounter; 
    private final List<Statistics> fsStats;
    
    @SuppressWarnings("unchecked")
    NewDirectOutputCollector(MRJobConfig jobContext,
        JobConf job, TaskUmbilicalProtocol umbilical, TaskReporter reporter) 
    throws IOException, ClassNotFoundException, InterruptedException {
      this.reporter = reporter;
      mapOutputRecordCounter = reporter
          .getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(FileOutputFormatCounter.BYTES_WRITTEN);

      List<Statistics> matchedStats = null;
      if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
        matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
            .getOutputPath(taskContext), taskContext.getConfiguration());
      }
      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes(fsStats);
      out = outputFormat.getRecordWriter(taskContext);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(K key, V value) 
    throws IOException, InterruptedException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }

    @Override
    public void close(TaskAttemptContext context) 
    throws IOException,InterruptedException {
      reporter.progress();
      if (out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(context);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }
    }
    
    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }
  
  private class NewOutputCollector<K,V>
    extends org.apache.hadoop.mapreduce.RecordWriter<K,V> {

    /**
     * NewOutputCollector的成员变量 MapOutputCollector 的具体实现类是 MapOutputBuffer
     * MapOutputBuffer的内部管理了一个100m大小的 byte[] kvbuffer（这个kvbuffer就是环形缓冲区的）
     *
     * MapOutputBuffer 的两个很重要的成员变量：
     *        byte[] kvbuffer
     *        SpillThread
     *
     */
    private final MapOutputCollector<K,V> collector;
    private final org.apache.hadoop.mapreduce.Partitioner<K,V> partitioner;
    private final int partitions;

    @SuppressWarnings("unchecked")
    NewOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
                       JobConf job,
                       TaskUmbilicalProtocol umbilical,
                       TaskReporter reporter
                       ) throws IOException, ClassNotFoundException {
      /**
       * collector的初始化
       */
      collector = createSortingCollector(job, reporter);

      /**
       * reduceTask的数量
       */
      partitions = jobContext.getNumReduceTasks();
      if (partitions > 1) {
        // 创建分区器
        // 默认是HashPartitioner
        partitioner = (org.apache.hadoop.mapreduce.Partitioner<K,V>)
          ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job);
      } else {
        // 如果只有一个reduceTask，那么每一次都是0号分区
        partitioner = new org.apache.hadoop.mapreduce.Partitioner<K,V>() {
          @Override
          public int getPartition(K key, V value, int numPartitions) {
            return partitions - 1;
          }
        };
      }
    }

    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      /**
       *  MapOutputBuffer
       *  在数据写入到环形缓冲区的时候，获取了分区信息：partitioner.getPartition(key, value, partitions)
       */
      collector.collect(key, value,
                        partitioner.getPartition(key, value, partitions));
    }

    @Override
    public void close(TaskAttemptContext context
                      ) throws IOException,InterruptedException {
      try {
        /**
         * 将缓冲区的数据进行flush
         */
        collector.flush();
      } catch (ClassNotFoundException cnf) {
        throw new IOException("can't find class ", cnf);
      }
      collector.close();
    }
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, ClassNotFoundException,
                             InterruptedException {


    /**
     * Task的上下文，管理着task相关的所有信息
     */
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, 
                                                                  getTaskID(),
                                                                  reporter);

    /**
     * Map Class  就是自己实现的那个Mapper
     * job.setMapperClass();
     * 反射获取实例
     *
     * 后面调用mapper.run方法
     */
    // make a mapper
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);

    /**
     * 获取InputFormat
     *
     */
    // make the input format
    org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE> inputFormat =
      (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);

    /**
     * 获取该mapTask要处理的InputSplit，封装了数据块相关的信息
     * 在进行逻辑切片的时候，FileInputFormat.getSplits() => List<InputSplit>
     */
    // rebuild the input split
    org.apache.hadoop.mapreduce.InputSplit split = null;
    split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
        splitIndex.getStartOffset());
    LOG.info("Processing split: " + split);

    /**
     * NewTrackingRecordReader 内部的real成员变量通过inputFormat.createRecordReader获取了
     * 真正负责读数据的LineRecordReader对象
     */
    org.apache.hadoop.mapreduce.RecordReader<INKEY,INVALUE> input =
      new NewTrackingRecordReader<INKEY,INVALUE>
        (split, inputFormat, reporter, taskContext);
    
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    org.apache.hadoop.mapreduce.RecordWriter output = null;


    // get an output object
    if (job.getNumReduceTasks() == 0) {
      /**
       * 没有reduce 直接将数据写到磁盘，没有排序，也没有combiner
       *
       */
      output = 
        new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
    } else {
      /**
       * 有reduce阶段 将数据写到环形缓冲区
       * 所以在mapper中的context.write()方法最终就是通过NewOutputCollector.collect()来处理的
       *
       */
      output = new NewOutputCollector(taskContext, job, umbilical, reporter);
    }

    org.apache.hadoop.mapreduce.MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> 
    mapContext = 
      new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, getTaskID(), 
          input, output, 
          committer, 
          reporter, split);

    /**
     * 包装了mapContext
     */
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
        mapperContext = 
          new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>().getMapContext(
              mapContext);

    try {
      /**
       * input =》 NewTrackingRecordReader
       * 初始化了对应的分片的文件输入流
       */
      input.initialize(split, mapperContext);
      /**
       * biglau
       * 自己写的map
       * run方法内部调用了我们实现的map方法
       * 执行业务逻辑
       */
      mapper.run(mapperContext);

      mapPhase.complete();
      setPhase(TaskStatus.Phase.SORT);
      statusUpdate(umbilical);
      input.close();
      input = null;
      /**
       * 内部会调用flush方法
       */
      output.close(mapperContext);
      output = null;
    } finally {
      closeQuietly(input);
      closeQuietly(output, mapperContext);
    }
  }

  class DirectMapOutputCollector<K, V>
    implements MapOutputCollector<K, V> {
 
    private RecordWriter<K, V> out = null;

    private TaskReporter reporter = null;

    private Counters.Counter mapOutputRecordCounter;
    private Counters.Counter fileOutputByteCounter;
    private List<Statistics> fsStats;

    public DirectMapOutputCollector() {
    }

    @SuppressWarnings("unchecked")
    public void init(MapOutputCollector.Context context
                    ) throws IOException, ClassNotFoundException {
      this.reporter = context.getReporter();
      JobConf job = context.getJobConf();
      String finalName = getOutputName(getPartition());
      FileSystem fs = FileSystem.get(job);

      OutputFormat<K, V> outputFormat = job.getOutputFormat();   
      mapOutputRecordCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
      
      fileOutputByteCounter = reporter
          .getCounter(FileOutputFormatCounter.BYTES_WRITTEN);

      List<Statistics> matchedStats = null;
      if (outputFormat instanceof FileOutputFormat) {
        matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
      }
      fsStats = matchedStats;

      long bytesOutPrev = getOutputBytes(fsStats);
      out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
    }

    public void close() throws IOException {
      if (this.out != null) {
        long bytesOutPrev = getOutputBytes(fsStats);
        out.close(this.reporter);
        long bytesOutCurr = getOutputBytes(fsStats);
        fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      }

    }

    public void flush() throws IOException, InterruptedException, 
                               ClassNotFoundException {
    }

    public void collect(K key, V value, int partition) throws IOException {
      reporter.progress();
      long bytesOutPrev = getOutputBytes(fsStats);
      out.write(key, value);
      long bytesOutCurr = getOutputBytes(fsStats);
      fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
      mapOutputRecordCounter.increment(1);
    }

    private long getOutputBytes(List<Statistics> stats) {
      if (stats == null) return 0;
      long bytesWritten = 0;
      for (Statistics stat: stats) {
        bytesWritten = bytesWritten + stat.getBytesWritten();
      }
      return bytesWritten;
    }
  }

  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  @InterfaceStability.Unstable
  public static class MapOutputBuffer<K extends Object, V extends Object>
      implements MapOutputCollector<K, V>, IndexedSortable {
    private int partitions;
    private JobConf job;
    private TaskReporter reporter;
    private Class<K> keyClass;
    private Class<V> valClass;
    private RawComparator<K> comparator;
    private SerializationFactory serializationFactory;
    private Serializer<K> keySerializer;
    private Serializer<V> valSerializer;
    private CombinerRunner<K,V> combinerRunner;
    private CombineOutputCollector<K, V> combineCollector;

    // Compression for map-outputs
    private CompressionCodec codec;

    // k/v accounting
    // 存放元数据
    private IntBuffer kvmeta; // metadata overlay on backing store
    int kvstart;            // marks origin of spill metadata
    int kvend;              // marks end of spill metadata
    int kvindex;            // marks end of fully serialized records

    // 分割元数据和写入kv数据的标识
    int equator;            // marks origin of meta/serialization

    // bufStart - bufEnd 就是要进行溢写的部分
    // kv数据的开始位置
    int bufstart;           // marks beginning of spill
    // kv数据的结束位置
    int bufend;             // marks beginning of collectable
    int bufmark;            // marks end of record
    int bufindex;           // marks end of collected
    // 存放数据的最大位置
    int bufvoid;            // marks the point where we should stop
                            // reading at the end of the buffer

    // 环形缓冲区，存放数据
    byte[] kvbuffer;        // main output buffer
    private final byte[] b0 = new byte[0];

    private static final int VALSTART = 0;         // val offset in acct
    private static final int KEYSTART = 1;         // key offset in acct
    private static final int PARTITION = 2;        // partition offset in acct
    private static final int VALLEN = 3;           // length of value
    private static final int NMETA = 4;            // num meta ints
    private static final int METASIZE = NMETA * 4; // size in bytes

    // spill accounting
    private int maxRec;
    private int softLimit;

    // 是否正在溢写数据
    boolean spillInProgress;
    // 还可以写输入数据的缓冲区大小
    int bufferRemaining;
    volatile Throwable sortSpillException = null;

    // 溢写次数
    int numSpills = 0;
    private int minSpillsForCombine;
    private IndexedSorter sorter;

    // 创建锁
    final ReentrantLock spillLock = new ReentrantLock();
    /**
     * 在MapTask工作的时候有两个重要的线程：
     *    一个是将数据写到环形缓冲区的线程（调用collector.collect()的线程）
     *    一个是进行溢写的线程
     *
     *    spillDone   表示spill完成了
     *    spillReady  表示可以开始spill了
     *
     */
    final Condition spillDone = spillLock.newCondition();
    final Condition spillReady = spillLock.newCondition();
    final BlockingBuffer bb = new BlockingBuffer();
    volatile boolean spillThreadRunning = false;
    /**
     * 溢写线程
     * mapTask中一共有两个线程：
     *              写数据的线程
     *              溢写线程
     * 初始化MapOutputBuffer的时候，就会启动这个线程，该线程一启动就会调用spillReady.await()等待spill的信号
     * 当环形缓冲区被写到80%的时候，写线程就会调用spillReady.signal()通知溢写线程可以开始溢写了。
     * 然后溢写线程就会开始对这80%的数据进行溢写，溢写的时候会先排序然后combiner。
     * 当溢写结束之后，溢写线程又会调用spillDone.signal()通知写线程。
     * 如果因为原来100mb的空间的20%也被写满了，那么写线程就会调用spillDone.await()等待溢写线程的通知
     */
    final SpillThread spillThread = new SpillThread();

    private FileSystem rfs;

    // Counters
    private Counters.Counter mapOutputByteCounter;
    private Counters.Counter mapOutputRecordCounter;
    private Counters.Counter fileOutputByteCounter;

    final ArrayList<SpillRecord> indexCacheList =
      new ArrayList<SpillRecord>();
    private int totalIndexCacheMemory;
    private int indexCacheMemoryLimit;
    private static final int INDEX_CACHE_MEMORY_LIMIT_DEFAULT = 1024 * 1024;

    private MapTask mapTask;
    private MapOutputFile mapOutputFile;
    private Progress sortPhase;
    private Counters.Counter spilledRecordsCounter;

    public MapOutputBuffer() {
    }

    @SuppressWarnings("unchecked")
    public void init(MapOutputCollector.Context context
                    ) throws IOException, ClassNotFoundException {
      job = context.getJobConf();
      reporter = context.getReporter();
      mapTask = context.getMapTask();
      mapOutputFile = mapTask.getMapOutputFile();
      sortPhase = mapTask.getSortPhase();
      spilledRecordsCounter = reporter.getCounter(TaskCounter.SPILLED_RECORDS);
      partitions = job.getNumReduceTasks();
      rfs = ((LocalFileSystem)FileSystem.getLocal(job)).getRaw();

      /**
       * spillper 溢写的阈值  默认80%     参数：mapreduce.map.sort.spill.percent
       * sortmb   环形缓冲区的大小  默认100MB   参数：mapreduce.task.io.sort.mb
       */
      //sanity checks
      final float spillper =
        job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float)0.8);
      final int sortmb = job.getInt(JobContext.IO_SORT_MB, 100);

      /**
       * 存放溢写文件的index的缓存大小：1024 * 1024   参数：mapreduce.task.index.cache.limit.bytes
       */
      indexCacheMemoryLimit = job.getInt(JobContext.INDEX_CACHE_MEMORY_LIMIT,
                                         INDEX_CACHE_MEMORY_LIMIT_DEFAULT);

      // 判断阈值的合法性(0.0, 1.0]
      if (spillper > (float)1.0 || spillper <= (float)0.0) {
        throw new IOException("Invalid \"" + JobContext.MAP_SORT_SPILL_PERCENT +
            "\": " + spillper);
      }

      // 判断环形缓冲区的大小的合法性 不能大于2048mb？？
      if ((sortmb & 0x7FF) != sortmb) {
        throw new IOException(
            "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
      }


      /**
       * 环形缓冲区排序算法：快排（QuickSort）   排序组件IndexedSorter
       * 磁盘文件的排序： 归并排序（MergeSort）
       */
      sorter = ReflectionUtils.newInstance(job.getClass("map.sort.class",
            QuickSort.class, IndexedSorter.class), job);


      /**
       * METASIZE = 16byte
       * sortmb = 100mb
       * 单位转换： 将100m => 104857600 byte
       */
      // buffers and accounting
      int maxMemUsage = sortmb << 20;
      // 计算buffer中最多有多少byte来存储元数据（一份元数据占16字节：VALSTART、KEYSTART、PARTITION、VALLEN）
      maxMemUsage -= maxMemUsage % METASIZE;
      /**
       * 初始化kvbuffer,以byte为单位
       */
      kvbuffer = new byte[maxMemUsage];
      // buf可写的最大位置
      bufvoid = kvbuffer.length;


      // kvbuffer转化为int型的kvmeta  以int为单位，也就是4byte
      kvmeta = ByteBuffer.wrap(kvbuffer)
         .order(ByteOrder.nativeOrder())
         .asIntBuffer();

      // 设置buf和元数据的分界线
      setEquator(0);
      bufstart = bufend = bufindex = equator;
      kvstart = kvend = kvindex;

      // kvmeta中存放元数据实体的最大个数
      maxRec = kvmeta.capacity() / NMETA;

      /**
       * 80%的环形缓冲区，超出这个限制就要开始溢写（不单单是sortmb*spillper）
       * kvbuffer.length 是一个16的整数倍，所以推荐将sortmb设置为16的整数倍
       */
      softLimit = (int)(kvbuffer.length * spillper);
      /**
       * 80% 中还剩下的可用大小
       */
      bufferRemaining = softLimit;
      LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
      LOG.info("soft limit at " + softLimit);
      LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "; length = " + maxRec);

      // k/v serialization
      comparator = job.getOutputKeyComparator();
      keyClass = (Class<K>)job.getMapOutputKeyClass();
      valClass = (Class<V>)job.getMapOutputValueClass();
      serializationFactory = new SerializationFactory(job);
      keySerializer = serializationFactory.getSerializer(keyClass);
      keySerializer.open(bb);
      valSerializer = serializationFactory.getSerializer(valClass);
      valSerializer.open(bb);

      // output counters
      mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
      mapOutputRecordCounter =
        reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
      fileOutputByteCounter = reporter
          .getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);

      // compression  压缩
      if (job.getCompressMapOutput()) {
        Class<? extends CompressionCodec> codecClass =
          job.getMapOutputCompressorClass(DefaultCodec.class);
        codec = ReflectionUtils.newInstance(codecClass, job);
      } else {
        codec = null;
      }

      // combiner
      final Counters.Counter combineInputCounter =
        reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
      /**
       * CombinerRunner
       */
      combinerRunner = CombinerRunner.create(job, getTaskID(), 
                                             combineInputCounter,
                                             reporter, null);
      if (combinerRunner != null) {
        final Counters.Counter combineOutputCounter =
          reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
        combineCollector= new CombineOutputCollector<K,V>(combineOutputCounter, reporter, job);
      } else {
        combineCollector = null;
      }
      spillInProgress = false;

      /**
       * 当溢写文件数量达到三个，就要开始合并
       */
      minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3);
      // 将溢写线程设置为守护线程
      spillThread.setDaemon(true);
      spillThread.setName("SpillThread");
      /**
       * 当前写数据线程获取到了锁
       */
      spillLock.lock();
      try {
        // 启动溢写线程
        spillThread.start();

        // 溢写线程是否处于running状态，默认一开始是false
        // 溢写线程启动之后，会将spillThreadRunning设置为true,然后调用spillDone.signal
        // 等待溢写线程启动
        while (!spillThreadRunning) {
          // 等待，await方法会释放锁。溢写线程唤醒写线程之后，写线程会等待获取锁，
          // 而后溢写线程会调用spillReady的await方法，阻塞并释放锁，写线程即可开始往下执行，
          // 此时spillThreadRunning已经为true，跳出循环，并释放锁。
          spillDone.await();
        }
      } catch (InterruptedException e) {
        throw new IOException("Spill thread failed to initialize", e);
      } finally {
        spillLock.unlock();
      }
      if (sortSpillException != null) {
        throw new IOException("Spill thread failed to initialize",
            sortSpillException);
      }
    }

    /**
     * Serialize the key, value to intermediate storage.
     * When this method returns, kvindex must refer to sufficient unused
     * storage to store one METADATA.
     */
    public synchronized void collect(K key, V value, final int partition
                                     ) throws IOException {
      reporter.progress();
      if (key.getClass() != keyClass) {
        throw new IOException("Type mismatch in key from map: expected "
                              + keyClass.getName() + ", received "
                              + key.getClass().getName());
      }
      if (value.getClass() != valClass) {
        throw new IOException("Type mismatch in value from map: expected "
                              + valClass.getName() + ", received "
                              + value.getClass().getName());
      }
      if (partition < 0 || partition >= partitions) {
        throw new IOException("Illegal partition for " + key + " (" +
            partition + ")");
      }
      checkSpillException();
      // 每次写数据之前，先减去16个字节的元数据占用的空间
      bufferRemaining -= METASIZE;


      /**
       * bufferRemaining表示100mb里面的80%
       * bufferRemaining <= 0 表示需要进行溢写
       */
      if (bufferRemaining <= 0) {
        // start spill if the thread is not running and the soft limit has been
        // reached
        spillLock.lock();
        try {
          do {

            // spillInProgress 默认是false
            if (!spillInProgress) {
              // 溢写线程没有在溢写数据

              // 将kvindex转换为byte为单位
              final int kvbidx = 4 * kvindex;
              // 将kvend 转换为byte为单位
              final int kvbend = 4 * kvend;
              // serialized, unspilled bytes always lie between kvindex and
              // bufindex, crossing the equator. Note that any void space
              // created by a reset must be included in "used" bytes
              // 计算被使用掉的大小，（包括元数据和输出数据）
              final int bUsed = distanceTo(kvbidx, bufindex);
              // 被使用的大小是否达到了溢写的阈值 80%
              final boolean bufsoftlimit = bUsed >= softLimit;

              if ((kvbend + METASIZE) % kvbuffer.length !=
                  equator - (equator % METASIZE)) {
                // spill finished, reclaim space
                /**
                 * 溢写完成
                 */
                resetSpill();
                bufferRemaining = Math.min(
                    distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                    softLimit - bUsed) - METASIZE;
                continue;
              } else if (bufsoftlimit && kvindex != kvend) {
                // spill records, if any collected; check latter, as it may
                // be possible for metadata alignment to hit spill pcnt
                /**
                 * 开始溢写  内部就是给溢写线程发送一个信号：可以开始溢写了
                 */
                startSpill();

                final int avgRec = (int)
                  (mapOutputByteCounter.getCounter() /
                  mapOutputRecordCounter.getCounter());
                // leave at least half the split buffer for serialization data
                // ensure that kvindex >= bufindex
                final int distkvi = distanceTo(bufindex, kvbidx);
                final int newPos = (bufindex +
                  Math.max(2 * METASIZE - 1,
                          Math.min(distkvi / 2,
                                   distkvi / (METASIZE + avgRec) * METASIZE)))
                  % kvbuffer.length;
                setEquator(newPos);
                bufmark = bufindex = newPos;
                final int serBound = 4 * kvend;
                // bytes remaining before the lock must be held and limits
                // checked is the minimum of three arcs: the metadata space, the
                // serialization space, and the soft limit
                bufferRemaining = Math.min(
                    // metadata max
                    distanceTo(bufend, newPos),
                    Math.min(
                      // serialization max
                      distanceTo(newPos, serBound),
                      // soft limit
                      softLimit)) - 2 * METASIZE;
              }
            }
          } while (false);
        } finally {
          spillLock.unlock();
        }
      }

      /**
       * 将数据写入环形缓冲区
       */
      try {
        // 先将key进行序列化
        // serialize key bytes into buffer
        int keystart = bufindex;
        keySerializer.serialize(key);

        if (bufindex < keystart) {
          // wrapped the key; must make contiguous
          bb.shiftBufferedKey();
          keystart = 0;
        }

        // serialize value bytes into buffer
        final int valstart = bufindex;
        valSerializer.serialize(value);
        // It's possible for records to have zero length, i.e. the serializer
        // will perform no writes. To ensure that the boundary conditions are
        // checked and that the kvindex invariant is maintained, perform a
        // zero-length write into the buffer. The logic monitoring this could be
        // moved into collect, but this is cleaner and inexpensive. For now, it
        // is acceptable.
        bb.write(b0, 0, 0);

        // the record must be marked after the preceding write, as the metadata
        // for this record are not yet written
        int valend = bb.markRecord();

        mapOutputRecordCounter.increment(1);
        mapOutputByteCounter.increment(
            distanceTo(keystart, valend, bufvoid));

        // write accounting info
        kvmeta.put(kvindex + PARTITION, partition);
        kvmeta.put(kvindex + KEYSTART, keystart);
        kvmeta.put(kvindex + VALSTART, valstart);
        kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
        // advance kvindex
        kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity();
      } catch (MapBufferTooSmallException e) {
        LOG.info("Record too large for in-memory buffer: " + e.getMessage());
        /**
         * 单条数据太大，直接溢写到磁盘
         */
        spillSingleRecord(key, value, partition);
        mapOutputRecordCounter.increment(1);
        return;
      }
    }

    private TaskAttemptID getTaskID() {
      return mapTask.getTaskID();
    }

    /**
     * Set the point from which meta and serialization data expand. The meta
     * indices are aligned with the buffer, so metadata never spans the ends of
     * the circular buffer.
     */
    private void setEquator(int pos) {
      equator = pos;
      // set index prior to first entry, aligned at meta boundary
      // 0 - 0 % 16 = 0
      final int aligned = pos - (pos % METASIZE);
      // Cast one of the operands to long to avoid integer overflow
      // (0 - 16 + len) % len / 4
      /**
       * (0 - 16 + len)  环形缓冲区从后往前（逆时针）走16个字节
       * (0 - 16 + len) % len  逆时针走16个字节的位置
       * (0 - 16 + len) % len / 4 剩余区域，除以4就是以4为单位划分，能划分多少个，因为kvbuffer是以字节为单位的，
       * 而元数据是以4个字节为单位的，这样求出来的就是存储元数据的下标
       *
       * eg：kvbuffer的长度是64，那么先减去16个位置，得到的就是48，这48个字节，
       * 以4个字节为单位划分就是12，那么对于元数据的存储来说，下一个坐标就是12。
       */
      kvindex = (int) (((long)aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
      LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
          "(" + (kvindex * 4) + ")");
    }

    /**
     * The spill is complete, so set the buffer and meta indices to be equal to
     * the new equator to free space for continuing collection. Note that when
     * kvindex == kvend == kvstart, the buffer is empty.
     */
    private void resetSpill() {
      final int e = equator;
      bufstart = bufend = e;
      final int aligned = e - (e % METASIZE);
      // set start/end to point to first meta record
      // Cast one of the operands to long to avoid integer overflow
      kvstart = kvend = (int)
        (((long)aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
      LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
        (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
    }

    /**
     * Compute the distance in bytes between two indices in the serialization
     * buffer.
     * @see #distanceTo(int,int,int)
     */
    final int distanceTo(final int i, final int j) {
      return distanceTo(i, j, kvbuffer.length);
    }

    /**
     * Compute the distance between two indices in the circular buffer given the
     * max distance.
     */
    int distanceTo(final int i, final int j, final int mod) {
      return i <= j
        ? j - i
        : mod - i + j;
    }

    /**
     * For the given meta position, return the offset into the int-sized
     * kvmeta buffer.
     */
    int offsetFor(int metapos) {
      return metapos * NMETA;
    }

    /**
     * Compare logical range, st i, j MOD offset capacity.
     * Compare by partition, then by key.
     * @see IndexedSortable#compare
     */
    public int compare(final int mi, final int mj) {
      final int kvi = offsetFor(mi % maxRec);
      final int kvj = offsetFor(mj % maxRec);
      final int kvip = kvmeta.get(kvi + PARTITION);
      final int kvjp = kvmeta.get(kvj + PARTITION);
      // sort by partition
      if (kvip != kvjp) {
        return kvip - kvjp;
      }
      // sort by key
      return comparator.compare(kvbuffer,
          kvmeta.get(kvi + KEYSTART),
          kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART),
          kvbuffer,
          kvmeta.get(kvj + KEYSTART),
          kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
    }

    final byte META_BUFFER_TMP[] = new byte[METASIZE];
    /**
     * Swap metadata for items i, j
     * @see IndexedSortable#swap
     */
    public void swap(final int mi, final int mj) {
      int iOff = (mi % maxRec) * METASIZE;
      int jOff = (mj % maxRec) * METASIZE;
      System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
      System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
      System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
    }

    /**
     * Inner class managing the spill of serialized records to disk.
     */
    protected class BlockingBuffer extends DataOutputStream {

      public BlockingBuffer() {
        super(new Buffer());
      }

      /**
       * Mark end of record. Note that this is required if the buffer is to
       * cut the spill in the proper place.
       */
      public int markRecord() {
        bufmark = bufindex;
        return bufindex;
      }

      /**
       * Set position from last mark to end of writable buffer, then rewrite
       * the data between last mark and kvindex.
       * This handles a special case where the key wraps around the buffer.
       * If the key is to be passed to a RawComparator, then it must be
       * contiguous in the buffer. This recopies the data in the buffer back
       * into itself, but starting at the beginning of the buffer. Note that
       * this method should <b>only</b> be called immediately after detecting
       * this condition. To call it at any other time is undefined and would
       * likely result in data loss or corruption.
       * @see #markRecord()
       */
      protected void shiftBufferedKey() throws IOException {
        // spillLock unnecessary; both kvend and kvindex are current
        int headbytelen = bufvoid - bufmark;
        bufvoid = bufmark;
        final int kvbidx = 4 * kvindex;
        final int kvbend = 4 * kvend;
        final int avail =
          Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
        if (bufindex + headbytelen < avail) {
          System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
          System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
          bufindex += headbytelen;
          bufferRemaining -= kvbuffer.length - bufvoid;
        } else {
          byte[] keytmp = new byte[bufindex];
          System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
          bufindex = 0;
          out.write(kvbuffer, bufmark, headbytelen);
          out.write(keytmp);
        }
      }
    }

    public class Buffer extends OutputStream {
      private final byte[] scratch = new byte[1];

      @Override
      public void write(int v)
          throws IOException {
        scratch[0] = (byte)v;
        write(scratch, 0, 1);
      }

      /**
       * Attempt to write a sequence of bytes to the collection buffer.
       * This method will block if the spill thread is running and it
       * cannot write.
       * @throws MapBufferTooSmallException if record is too large to
       *    deserialize into the collection buffer.
       */
      @Override
      public void write(byte b[], int off, int len)
          throws IOException {
        // must always verify the invariant that at least METASIZE bytes are
        // available beyond kvindex, even when len == 0
        bufferRemaining -= len;
        if (bufferRemaining <= 0) {
          // writing these bytes could exhaust available buffer space or fill
          // the buffer to soft limit. check if spill or blocking are necessary
          boolean blockwrite = false;
          spillLock.lock();
          try {
            do {
              checkSpillException();

              final int kvbidx = 4 * kvindex;
              final int kvbend = 4 * kvend;
              // ser distance to key index
              final int distkvi = distanceTo(bufindex, kvbidx);
              // ser distance to spill end index
              final int distkve = distanceTo(bufindex, kvbend);

              // if kvindex is closer than kvend, then a spill is neither in
              // progress nor complete and reset since the lock was held. The
              // write should block only if there is insufficient space to
              // complete the current write, write the metadata for this record,
              // and write the metadata for the next record. If kvend is closer,
              // then the write should block if there is too little space for
              // either the metadata or the current write. Note that collect
              // ensures its metadata requirement with a zero-length write
              blockwrite = distkvi <= distkve
                ? distkvi <= len + 2 * METASIZE
                : distkve <= len || distanceTo(bufend, kvbidx) < 2 * METASIZE;

              if (!spillInProgress) {
                if (blockwrite) {
                  if ((kvbend + METASIZE) % kvbuffer.length !=
                      equator - (equator % METASIZE)) {
                    // spill finished, reclaim space
                    // need to use meta exclusively; zero-len rec & 100% spill
                    // pcnt would fail
                    resetSpill(); // resetSpill doesn't move bufindex, kvindex
                    bufferRemaining = Math.min(
                        distkvi - 2 * METASIZE,
                        softLimit - distanceTo(kvbidx, bufindex)) - len;
                    continue;
                  }
                  // we have records we can spill; only spill if blocked
                  if (kvindex != kvend) {
                    startSpill();
                    // Blocked on this write, waiting for the spill just
                    // initiated to finish. Instead of repositioning the marker
                    // and copying the partial record, we set the record start
                    // to be the new equator
                    setEquator(bufmark);
                  } else {
                    // We have no buffered records, and this record is too large
                    // to write into kvbuffer. We must spill it directly from
                    // collect
                    final int size = distanceTo(bufstart, bufindex) + len;
                    setEquator(0);
                    bufstart = bufend = bufindex = equator;
                    kvstart = kvend = kvindex;
                    bufvoid = kvbuffer.length;
                    throw new MapBufferTooSmallException(size + " bytes");
                  }
                }
              }

              if (blockwrite) {
                // wait for spill
                try {
                  while (spillInProgress) {
                    reporter.progress();
                    spillDone.await();
                  }
                } catch (InterruptedException e) {
                    throw new IOException(
                        "Buffer interrupted while waiting for the writer", e);
                }
              }
            } while (blockwrite);
          } finally {
            spillLock.unlock();
          }
        }
        // here, we know that we have sufficient space to write
        if (bufindex + len > bufvoid) {
          final int gaplen = bufvoid - bufindex;
          System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
          len -= gaplen;
          off += gaplen;
          bufindex = 0;
        }
        System.arraycopy(b, off, kvbuffer, bufindex, len);
        bufindex += len;
      }
    }

    public void flush() throws IOException, ClassNotFoundException,
           InterruptedException {
      LOG.info("Starting flush of map output");
      if (kvbuffer == null) {
        LOG.info("kvbuffer is null. Skipping flush.");
        return;
      }
      spillLock.lock();
      try {
        /**
         * 溢写线程正在溢写
         */
        while (spillInProgress) {
          reporter.progress();
          spillDone.await();
        }
        checkSpillException();

        final int kvbend = 4 * kvend;
        if ((kvbend + METASIZE) % kvbuffer.length !=
            equator - (equator % METASIZE)) {
          // spill finished
          resetSpill();
        }
        if (kvindex != kvend) {
          kvend = (kvindex + NMETA) % kvmeta.capacity();
          bufend = bufmark;
          LOG.info("Spilling map output");
          LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                   "; bufvoid = " + bufvoid);
          LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                   "); kvend = " + kvend + "(" + (kvend * 4) +
                   "); length = " + (distanceTo(kvend, kvstart,
                         kvmeta.capacity()) + 1) + "/" + maxRec);
          /**
           * 缓冲区中还有数据
           */
          sortAndSpill();
        }
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for the writer", e);
      } finally {
        spillLock.unlock();
      }
      assert !spillLock.isHeldByCurrentThread();
      // shut down spill thread and wait for it to exit. Since the preceding
      // ensures that it is finished with its work (and sortAndSpill did not
      // throw), we elect to use an interrupt instead of setting a flag.
      // Spilling simultaneously from this thread while the spill thread
      // finishes its work might be both a useful way to extend this and also
      // sufficient motivation for the latter approach.
      try {
        spillThread.interrupt();
        spillThread.join();
      } catch (InterruptedException e) {
        throw new IOException("Spill failed", e);
      }
      // release sort buffer before the merge
      kvbuffer = null;
      /**
       * 对溢写文件进行merge
       */
      mergeParts();
      Path outputPath = mapOutputFile.getOutputFile();
      fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
    }

    public void close() { }

    protected class SpillThread extends Thread {

      @Override
      public void run() {
        spillLock.lock();
        spillThreadRunning = true;
        try {
          while (true) {
            // 通知写数据线程，可以开始写数据了
            spillDone.signal();

            // spillInProgress 初始值为false
            while (!spillInProgress) {
              // 阻塞溢写线程,释放锁
              spillReady.await();
            }
            try {
              spillLock.unlock();
              /**
               * 开始溢写  sort  combiner spill
               * 由于这里是循环，所以写完了之后就开始新的循环，就会通知写线程spillDone.signal();
               */
              sortAndSpill();
            } catch (Throwable t) {
              sortSpillException = t;
            } finally {
              spillLock.lock();
              if (bufend < bufstart) {
                bufvoid = kvbuffer.length;
              }
              kvstart = kvend;
              bufstart = bufend;
              spillInProgress = false;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } finally {
          spillLock.unlock();
          spillThreadRunning = false;
        }
      }
    }

    private void checkSpillException() throws IOException {
      final Throwable lspillException = sortSpillException;
      if (lspillException != null) {
        if (lspillException instanceof Error) {
          final String logMsg = "Task " + getTaskID() + " failed : " +
            StringUtils.stringifyException(lspillException);
          mapTask.reportFatalError(getTaskID(), lspillException, logMsg);
        }
        throw new IOException("Spill failed", lspillException);
      }
    }

    private void startSpill() {
      // 断言溢写线程现在没有在溢写数据
      assert !spillInProgress;
      kvend = (kvindex + NMETA) % kvmeta.capacity();
      bufend = bufmark;
      spillInProgress = true;
      LOG.info("Spilling map output");
      LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
               "; bufvoid = " + bufvoid);
      LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
               "); kvend = " + kvend + "(" + (kvend * 4) +
               "); length = " + (distanceTo(kvend, kvstart,
                     kvmeta.capacity()) + 1) + "/" + maxRec);
      /**
       * 通知溢写线程可以开始写了
       */
      spillReady.signal();
    }

    private void sortAndSpill() throws IOException, ClassNotFoundException,
                                       InterruptedException {
      //approximate the length of the output file to be the length of the
      //buffer + header lengths for the partitions
      final long size = distanceTo(bufstart, bufend, bufvoid) +
                  partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        // SpillRecord 存储溢写文件的的索引信息
        final SpillRecord spillRec = new SpillRecord(partitions);

        /**
         * 创建一个溢写的文件  mapOutputFile
         * 命名规则： spill + numSpills + .out
         */
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);

        final int mstart = kvend / NMETA;
        final int mend = 1 + // kvend is a valid record
          (kvstart >= kvend
          ? kvstart
          : kvmeta.capacity() + kvstart) / NMETA;

        /**
         * ！！！！！！！！！！！！！！！
         * ！！！！！！！！！！！！！！！
         * 排序，这里是快排
         * ！！！！！！！！！！！！！！！
         * mstart, mend,   环形缓冲区中溢写的范围
         *
         * 排序的时候实际是按照partition和key的联合排序
         */
        sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);

        int spindex = mstart;
        /**
         * IndexRecord 当前这个分区的索引数据
         * 一个IndexRecorde占24字节
         */
        final IndexRecord rec = new IndexRecord();
        final InMemValBytes value = new InMemValBytes();

        /**
         * 遍历分区，按分区ID从小到大写数据
         */
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            // 当前这个分区的segment的开始位置
            long segmentStart = out.getPos();

            FSDataOutputStream partitionOut = CryptoUtils.wrapIfNecessary(job, out);

            writer = new Writer<K, V>(job, partitionOut, keyClass, valClass, codec,
                                      spilledRecordsCounter);

            /**
             * 判断是否是combiner，如果有会进行局部聚合
             */
            if (combinerRunner == null) {
              // spill directly
              DataInputBuffer key = new DataInputBuffer();
              while (spindex < mend &&
                  kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
                final int kvoff = offsetFor(spindex % maxRec);
                int keystart = kvmeta.get(kvoff + KEYSTART);
                int valstart = kvmeta.get(kvoff + VALSTART);
                key.reset(kvbuffer, keystart, valstart - keystart);
                getVBytesForOffset(kvoff, value);
                /**
                 * 没有combine直接写出
                 */
                writer.append(key, value);
                ++spindex;
              }
            } else {
              int spstart = spindex;
              while (spindex < mend &&
                  kvmeta.get(offsetFor(spindex % maxRec)
                            + PARTITION) == i) {
                ++spindex;
              }
              // Note: we would like to avoid the combiner if we've fewer
              // than some threshold of records for a partition
              if (spstart != spindex) {
                combineCollector.setWriter(writer);
                RawKeyValueIterator kvIter =
                  new MRResultIterator(spstart, spindex);
                /**
                 * 有combiner  先执行聚合操作
                 * 里面的逻辑就是对这个分区的数据先执行了一次reduce（combine设置的reduce）操作。
                 * 然后在将数据写出去
                 */
                combinerRunner.combine(kvIter, combineCollector);
              }
            }

            /**
             *
             */
            // close the writer
            writer.close();

            // record offsets
            // 端偏移（分区数据开始的位置）
            rec.startOffset = segmentStart;
            // 原始数据长度
            rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
            // 压缩之后数据长度
            rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);

            /**
             * SpillRecord : 内部存储分区的索引信息：IndexRecorde
             * 一个溢写文件包含多个分区的数据，每一个分区都有一个对饮IndexRecorde
             * 里面封装了该分区的索引数据
             */
            spillRec.putIndex(rec, i);

            writer = null;
          } finally {
            if (null != writer) writer.close();
          }
        }

        /**
         * 索引数据在内存中放不下了，需要放到
         */
        if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);

          /**
           * 将索引数据写到文件
           */
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        LOG.info("Finished spill " + numSpills);
        ++numSpills;
      } finally {
        /**
         *
         */
        if (out != null) out.close();
      }
    }

    /**
     * Handles the degenerate case where serialization fails to fit in
     * the in-memory buffer, so we must spill the record from collect
     * directly to a spill file. Consider this "losing".
     */
    private void spillSingleRecord(final K key, final V value,
                                   int partition) throws IOException {
      long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
      FSDataOutputStream out = null;
      try {
        // create spill file
        final SpillRecord spillRec = new SpillRecord(partitions);
        final Path filename =
            mapOutputFile.getSpillFileForWrite(numSpills, size);
        out = rfs.create(filename);

        // we don't run the combiner for a single record
        IndexRecord rec = new IndexRecord();
        for (int i = 0; i < partitions; ++i) {
          IFile.Writer<K, V> writer = null;
          try {
            long segmentStart = out.getPos();
            // Create a new codec, don't care!
            FSDataOutputStream partitionOut = CryptoUtils.wrapIfNecessary(job, out);
            writer = new IFile.Writer<K,V>(job, partitionOut, keyClass, valClass, codec,
                                            spilledRecordsCounter);

            if (i == partition) {
              final long recordStart = out.getPos();
              writer.append(key, value);
              // Note that our map byte count will not be accurate with
              // compression
              mapOutputByteCounter.increment(out.getPos() - recordStart);
            }
            writer.close();

            // record offsets
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
            rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
            spillRec.putIndex(rec, i);

            writer = null;
          } catch (IOException e) {
            if (null != writer) writer.close();
            throw e;
          }
        }
        if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
          // create spill index file
          Path indexFilename =
              mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                  * MAP_OUTPUT_INDEX_RECORD_LENGTH);
          spillRec.writeToFile(indexFilename, job);
        } else {
          indexCacheList.add(spillRec);
          totalIndexCacheMemory +=
            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
        }
        ++numSpills;
      } finally {
        if (out != null) out.close();
      }
    }

    /**
     * Given an offset, populate vbytes with the associated set of
     * deserialized value bytes. Should only be called during a spill.
     */
    private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
      // get the keystart for the next serialized value to be the end
      // of this value. If this is the last value in the buffer, use bufend
      final int vallen = kvmeta.get(kvoff + VALLEN);
      assert vallen >= 0;
      vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
    }

    /**
     * Inner class wrapping valuebytes, used for appendRaw.
     */
    protected class InMemValBytes extends DataInputBuffer {
      private byte[] buffer;
      private int start;
      private int length;

      public void reset(byte[] buffer, int start, int length) {
        this.buffer = buffer;
        this.start = start;
        this.length = length;

        if (start + length > bufvoid) {
          this.buffer = new byte[this.length];
          final int taillen = bufvoid - start;
          System.arraycopy(buffer, start, this.buffer, 0, taillen);
          System.arraycopy(buffer, 0, this.buffer, taillen, length-taillen);
          this.start = 0;
        }

        super.reset(this.buffer, this.start, this.length);
      }
    }

    protected class MRResultIterator implements RawKeyValueIterator {
      private final DataInputBuffer keybuf = new DataInputBuffer();
      private final InMemValBytes vbytes = new InMemValBytes();
      private final int end;
      private int current;
      public MRResultIterator(int start, int end) {
        this.end = end;
        current = start - 1;
      }
      public boolean next() throws IOException {
        return ++current < end;
      }
      public DataInputBuffer getKey() throws IOException {
        final int kvoff = offsetFor(current % maxRec);
        keybuf.reset(kvbuffer, kvmeta.get(kvoff + KEYSTART),
            kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART));
        return keybuf;
      }
      public DataInputBuffer getValue() throws IOException {
        getVBytesForOffset(offsetFor(current % maxRec), vbytes);
        return vbytes;
      }
      public Progress getProgress() {
        return null;
      }
      public void close() { }
    }

    private void mergeParts() throws IOException, InterruptedException, 
                                     ClassNotFoundException {
      // get the approximate size of the final output/index files
      long finalOutFileSize = 0;
      long finalIndexFileSize = 0;
      final Path[] filename = new Path[numSpills];
      final TaskAttemptID mapId = getTaskID();

      for(int i = 0; i < numSpills; i++) {
        filename[i] = mapOutputFile.getSpillFile(i);
        finalOutFileSize += rfs.getFileStatus(filename[i]).getLen();
      }

      if (numSpills == 1) { //the spill is the final output
        sameVolRename(filename[0],
            mapOutputFile.getOutputFileForWriteInVolume(filename[0]));
        if (indexCacheList.size() == 0) {
          sameVolRename(mapOutputFile.getSpillIndexFile(0),
            mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]));
        } else {
          indexCacheList.get(0).writeToFile(
            mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]), job);
        }
        sortPhase.complete();
        return;
      }

      // read in paged indices
      for (int i = indexCacheList.size(); i < numSpills; ++i) {
        Path indexFileName = mapOutputFile.getSpillIndexFile(i);
        indexCacheList.add(new SpillRecord(indexFileName, job));
      }

      //make correction in the length to include the sequence file header
      //lengths for each partition
      finalOutFileSize += partitions * APPROX_HEADER_LENGTH;
      finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      Path finalOutputFile =
          mapOutputFile.getOutputFileForWrite(finalOutFileSize);
      Path finalIndexFile =
          mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

      //The output stream for the final single output file
      /**
       * 最后一个输出文件
       */
      FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);

      if (numSpills == 0) {
        //create dummy files
        IndexRecord rec = new IndexRecord();
        SpillRecord sr = new SpillRecord(partitions);
        try {
          for (int i = 0; i < partitions; i++) {
            long segmentStart = finalOut.getPos();
            FSDataOutputStream finalPartitionOut = CryptoUtils.wrapIfNecessary(job, finalOut);
            Writer<K, V> writer =
              new Writer<K, V>(job, finalPartitionOut, keyClass, valClass, codec, null);
            writer.close();
            rec.startOffset = segmentStart;
            rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
            rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
            sr.putIndex(rec, i);
          }
          sr.writeToFile(finalIndexFile, job);
        } finally {
          finalOut.close();
        }
        sortPhase.complete();
        return;
      }
      {
        sortPhase.addPhases(partitions); // Divide sort phase into sub-phases
        
        IndexRecord rec = new IndexRecord();
        final SpillRecord spillRec = new SpillRecord(partitions);

        /**
         * 一次遍历每一个分区，然后遍历所有的溢写文件，将对应分区在每一个溢写文件中的索引信息找到，
         * 为每一个文件中的该分区对应的数据创建一个Segment对象，并将其添加到segmentList中
         */
        for (int parts = 0; parts < partitions; parts++) {
          //create the segments to be merged
          List<Segment<K,V>> segmentList =
            new ArrayList<Segment<K, V>>(numSpills);
          for(int i = 0; i < numSpills; i++) {
            IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

            Segment<K,V> s =
              new Segment<K,V>(job, rfs, filename[i], indexRecord.startOffset,
                               indexRecord.partLength, codec, true);
            segmentList.add(i, s);

            if (LOG.isDebugEnabled()) {
              LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                  "Spill =" + i + "(" + indexRecord.startOffset + "," +
                  indexRecord.rawLength + ", " + indexRecord.partLength + ")");
            }
          }

          int mergeFactor = job.getInt(JobContext.IO_SORT_FACTOR, 100);
          // sort the segments only if there are intermediate merges
          // segmentList.size() > mergeFactor  默认情况下表示当前分区的数据分布在一百个segment中
          // 当前分区的segment数量大于mapreduce.task.io.sort.factor merge阈值 默认100 至少有一百个溢写文件
          boolean sortSegments = segmentList.size() > mergeFactor;
          //merge
          @SuppressWarnings("unchecked")
          // 对当前分区的所有segment进行merge
          RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                         keyClass, valClass, codec,
                         segmentList, mergeFactor,
                         new Path(mapId.toString()),
                         job.getOutputKeyComparator(), reporter, sortSegments,
                         null, spilledRecordsCounter, sortPhase.phase(),
                         TaskType.MAP);

          //write merged output to disk
          long segmentStart = finalOut.getPos();
          FSDataOutputStream finalPartitionOut = CryptoUtils.wrapIfNecessary(job, finalOut);
          // 为当前分区创建一个Writer对象
          Writer<K, V> writer =
              new Writer<K, V>(job, finalPartitionOut, keyClass, valClass, codec,
                               spilledRecordsCounter);

          if (combinerRunner == null || numSpills < minSpillsForCombine) {
            // 不需要进行combiner
            // 直接将数据写出
            Merger.writeFile(kvIter, writer, reporter, job);
          } else {
            // 需要对数据进行combiner
            combineCollector.setWriter(writer);
            combinerRunner.combine(kvIter, combineCollector);
          }

          //close
          writer.close();

          sortPhase.startNextPhase();
          
          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
          rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
          spillRec.putIndex(rec, parts);
        }
        spillRec.writeToFile(finalIndexFile, job);
        finalOut.close();
        for(int i = 0; i < numSpills; i++) {
          rfs.delete(filename[i],true);
        }
      }
    }
    
    /**
     * Rename srcPath to dstPath on the same volume. This is the same
     * as RawLocalFileSystem's rename method, except that it will not
     * fall back to a copy, and it will create the target directory
     * if it doesn't exist.
     */
    private void sameVolRename(Path srcPath,
        Path dstPath) throws IOException {
      RawLocalFileSystem rfs = (RawLocalFileSystem)this.rfs;
      File src = rfs.pathToFile(srcPath);
      File dst = rfs.pathToFile(dstPath);
      if (!dst.getParentFile().exists()) {
        if (!dst.getParentFile().mkdirs()) {
          throw new IOException("Unable to rename " + src + " to "
              + dst + ": couldn't create parent directory"); 
        }
      }
      
      if (!src.renameTo(dst)) {
        throw new IOException("Unable to rename " + src + " to " + dst);
      }
    }
  } // MapOutputBuffer
  
  /**
   * Exception indicating that the allocated sort buffer is insufficient
   * to hold the current record.
   */
  @SuppressWarnings("serial")
  private static class MapBufferTooSmallException extends IOException {
    public MapBufferTooSmallException(String s) {
      super(s);
    }
  }

  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void closeQuietly(RecordReader<INKEY, INVALUE> c) {
    if (c != null) {
      try {
        c.close();
      } catch (IOException ie) {
        // Ignore
        LOG.info("Ignoring exception during close for " + c, ie);
      }
    }
  }
  
  private <OUTKEY, OUTVALUE>
  void closeQuietly(MapOutputCollector<OUTKEY, OUTVALUE> c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception ie) {
        // Ignore
        LOG.info("Ignoring exception during close for " + c, ie);
      }
    }
  }
  
  private <INKEY, INVALUE, OUTKEY, OUTVALUE>
  void closeQuietly(
      org.apache.hadoop.mapreduce.RecordReader<INKEY, INVALUE> c) {
    if (c != null) {
      try {
        c.close();
      } catch (Exception ie) {
        // Ignore
        LOG.info("Ignoring exception during close for " + c, ie);
      }
    }
  }

  private <INKEY, INVALUE, OUTKEY, OUTVALUE>
  void closeQuietly(
      org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> c,
      org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context
          mapperContext) {
    if (c != null) {
      try {
        c.close(mapperContext);
      } catch (Exception ie) {
        // Ignore
        LOG.info("Ignoring exception during close for " + c, ie);
      }
    }
  }
}
