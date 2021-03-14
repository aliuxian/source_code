/*
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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 *
 * <p>This ResourceManager doesn't acquire new resources.
 */
public class StandaloneResourceManager extends ResourceManager<ResourceID> {

    /** The duration of the startup period. A duration of zero means there is no startup period. */
    private final Time startupPeriodTime;

    public StandaloneResourceManager(
            RpcService rpcService,
            ResourceID resourceId,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Time startupPeriodTime,
            Time rpcTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                resourceId,
                highAvailabilityServices,
                heartbeatServices,
                slotManager,
                clusterPartitionTrackerFactory,
                jobLeaderIdService,
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                rpcTimeout,
                ioExecutor);
        this.startupPeriodTime = Preconditions.checkNotNull(startupPeriodTime);
    }

    @Override
    protected void initialize() throws ResourceManagerException {
        // nothing to initialize
    }

    @Override
    protected void terminate() {
        // noop
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String diagnostics) {}

    @Override
    public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
        return false;
    }

    @Override
    public boolean stopWorker(ResourceID resourceID) {
        // standalone resource manager cannot stop workers
        return false;
    }

    @Override
    protected ResourceID workerStarted(ResourceID resourceID) {
        return resourceID;
    }

    @Override
    protected void onLeadership() {
        /**
         * 主节点JobManager启动好了之后，等待一段时间之后还是没有TaskManager来注册
         *
         * 开始 启动期
         * 刚开始启动的时候不会去将无法满足的Slot请求置为失败，
         * 但是等待一段时间（默认5minutes）之后就会将所有的无法满足的slot 请求置为失败
         */
        startStartupPeriod();
    }

    private void startStartupPeriod() {

        /**
         *是否将无法满足的Slot Request设置为失败
         *
         * 会将SlotManagerImpl 的 failUnfulfillableRequest 设置为false。默认为true
         * 为true的时候，会立即将无法满足的Slot request置为失败。
         */
        setFailUnfulfillableRequest(false);

        final long startupPeriodMillis = startupPeriodTime.toMilliseconds();

        // 启动一个异步定时任务
        /**
         * 超过startupPeriodMillis就会将没有得到分配的Slot请求设置为失败
         */
        if (startupPeriodMillis > 0) {
            scheduleRunAsync(
                    () -> setFailUnfulfillableRequest(true),
                    startupPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }
}
