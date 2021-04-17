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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.Time;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;
  
  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  
  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {

    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>();

    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      });
    }

    return qr;
  }
  
  private QuorumCall() {
    // Only instantiated from factory method above
  }
  
  /**
   * Wait for the quorum to achieve a certain number of responses.
   * 
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
  /**
   * 假如有五个jn
   * minResponses = 5
   * minSuccesses = 3
   * maxExceptions = 3
   * millis = 20s默认
   */
      throws InterruptedException, TimeoutException {

    // 当前时间
    long st = Time.monotonicNow();

    // 计算打印日志的时候
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);

    // 超时的时间点
    long et = st + millis;


    /**
     * 解决FullGC带来的问题
     *
     * 在进入while循环之后，获取一个时间，记为start，
     * 在执行到rem < 0判断的时候，获取一个时间，记为end，然后判断 end和start的差值，是否大于一个预估的执行时间，比如5s。
     * 如果大于的话，说明发生了Full GC，那么就应该将超时时间更新一下，让他将full GC的时间排除。
     * 这个预估的值推荐使用参数来配置，避免硬编码
     *
     * 因为这个中间的部分没有什么很复杂的操作，所以他们的执行时间可能就是ms级别的。所以如果执行时间过长，那么肯定是发生了Full GC
     *
     * 对于Full GC这个问题，其实在很多分布式的系统中都是需要考虑的
     */
    while (true) {
      checkAssertionErrors();
      // minResponses = jn个数
      // 返回的响应超过jn的个数，返回
      if (minResponses > 0 && countResponses() >= minResponses) return;
      // 成功的数量大于等于一半的jn数量（eg：3）
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      // 失败的响应数量大于一半的jn数量（eg：3）
      // 这里应该是大于等于？？？？
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;

      /**
       * 假如在这发生了Full GC   STW
       *
       * NameNode的内存你都比较大，在一极端情况下，full GC 的时间可能会很长，
       * 可能直接停止的时间直接炒超过了超时时间，那么下面就直接超时了
       * ！！！！  这个时候退出并不是因为jn超时了，而是一个gc导致的，这不太合理
       *
       * 解决方法：
       * 将Full GC导致的stw的时间减去
       */


      /**
       * 获取当前时间
       *
       */
      long now = Time.monotonicNow();

      if (now > nextLogTime) {
        // 需要打印日志

        // waited 从开始等待响应到现在花费的时间
        long waited = now - st;
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);

        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }

        // millis * WAIT_PROGRESS_WARN_THRESHOLD = 20 * 0.7
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          QuorumJournalManager.LOG.info(msg);
        }
        // 下一次打印日志的时间，比现在晚一秒。第一次等了6秒才打印，这里开始只等一秒
        // 说明可能出问题了，加快打印日志的方式提醒用户
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }

      // 距离超时还剩下的时间
      long rem = et - now;

      // 超时了，抛出异常
      if (rem <= 0) {
        throw new TimeoutException();
      }


      /**
       * 第一次 nextLogTime - now会比较久一点
       * 后面每一次都是一秒。
       */
      rem = Math.min(rem, nextLogTime - now);
      rem = Math.max(rem, 1);
      wait(rem);

      // 这里也可能发生Full GC
      // 但是gc结束之后，还是会去执行判断响应情况，，可能这个时候已经得到正确的结果了
      // 也可能没有得到正确的响应结果，那么程序还是会直接退出，所以这里也是需要考虑full GC
    }
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   * 
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing through.
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }
  
  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }
  
  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }
  
  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }
  
  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}
