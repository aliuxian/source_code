/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import org.apache.spark.{Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus

/**
 * The interface for customizing shuffle write process. The driver create a ShuffleWriteProcessor
 * and put it into [[ShuffleDependency]], and executors use it in each ShuffleMapTask.
 */
private[spark] class ShuffleWriteProcessor extends Serializable with Logging {

  /**
   * Create a [[ShuffleWriteMetricsReporter]] from the task context. As the reporter is a
   * per-row operator, here need a careful consideration on performance.
   */
  protected def createMetricsReporter(context: TaskContext): ShuffleWriteMetricsReporter = {
    context.taskMetrics().shuffleWriteMetrics
  }

  /**
   * The write process for particular partition, it controls the life circle of [[ShuffleWriter]]
   * get from [[ShuffleManager]] and triggers rdd compute, finally return the [[MapStatus]] for
   * this task.
   */
  def write(
      rdd: RDD[_],
      dep: ShuffleDependency[_, _, _],
      mapId: Long,
      context: TaskContext,
      partition: Partition): MapStatus = {
    var writer: ShuffleWriter[Any, Any] = null
    try {
      /**
       * shuffleManager   就是SortShuffleManager
       */
      val manager = SparkEnv.get.shuffleManager

      /**
       * 获取shuffle过程中的writer
       * 三种实现：
       *
       */
      writer = manager.getWriter[Any, Any](
        dep.shuffleHandle,
        mapId,
        context,
        createMetricsReporter(context))

      /**
       * writer 有三种实现：
       * 普通机制：和mr的一样
       * bypass机制：不需要map聚合  分区数小于200
       * unsafe：不需要map聚合，分区数小于16777216，Serializer支持relocation
       *
       * 最后每个task只会生成一个数据文件，以及一个索引文件
       *
       * 执行计算逻辑，并将数据写出去
       * 执行rdd 中 partition 的数据计算逻辑
       */
      writer.write(
        rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any, Any]]])

      /**
       * 删除spill过程中产生的文件（用不到的那部分），释放内存和磁盘资源，
       * 比如：将一些spill文件合并之后，原先的文件就可以删除了
       */
      writer.stop(success = true).get
    } catch {
      case e: Exception =>
        try {
          if (writer != null) {
            writer.stop(success = false)
          }
        } catch {
          case e: Exception =>
            log.debug("Could not stop writer", e)
        }
        throw e
    }
  }
}
