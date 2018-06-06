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

package org.apache.hadoop.yarn.server.timelineservice.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Metrics class for TimelineCollector.
 */
@Metrics(about = "Metrics for timeline collector", context = "timelineservice")
public class TimelineCollectorMetrics {

  private static final MetricsInfo METRICS_INFO = info("TimelineCollectorMetrics",
      "Metrics for TimelineCollector");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static volatile TimelineCollectorMetrics instance = null;
  private final MetricsRegistry registry;
  @Metric("PUT entities total count")
  private MutableCounterInt putEntitiesTotalCount;
  @Metric("PUT entities success count")
  private MutableCounterInt putEntitiesSuccessCount;
  @Metric("PUT entities failure count")
  private MutableCounterInt putEntitiesFailureCount;
  @Metric("async PUT entities total count")
  private MutableCounterInt asyncPutEntitiesTotalCount;
  @Metric("async PUT entities success count")
  private MutableCounterInt asyncPutEntitiesSuccessCount;
  @Metric("async PUT entities failure count")
  private MutableCounterInt asyncPutEntitiesFailureCount;

  private MutableQuantiles putEntitiesLatency;
  private MutableQuantiles putEntitiesSuccessLatency;
  private MutableQuantiles putEntitiesFailureLatency;
  private MutableQuantiles asyncPutEntitiesLatency;
  private MutableQuantiles asyncPutEntitiesSuccessLatency;
  private MutableQuantiles asyncPutEntitiesFailureLatency;

  private TimelineCollectorMetrics() {
    this.registry = new MetricsRegistry(METRICS_INFO);
    this.putEntitiesLatency =
        registry.newQuantiles("putEntitiesLatency",
            "PUT entities latency", "ops", "latency", 10);
    this.putEntitiesSuccessLatency =
        registry.newQuantiles("putEntitiesSuccessLatency",
            "PUT entities success latency", "ops", "latency", 10);
    this.putEntitiesFailureLatency =
        registry.newQuantiles("putEntitiesFailureLatency",
            "PUT entities failure latency", "ops", "latency", 10);

    this.asyncPutEntitiesLatency =
        registry.newQuantiles("asyncPutEntitiesLatency",
            "async PUT entities latency", "ops", "latency", 10);
    this.asyncPutEntitiesSuccessLatency =
        registry.newQuantiles("asyncPutEntitiesSuccessLatency",
            "async PUT entities success latency", "ops", "latency", 10);
    this.asyncPutEntitiesFailureLatency =
        registry.newQuantiles("asyncPutEntitiesFailureLatency",
            "async PUT entities failure latency", "ops", "latency", 10);
  }

  public static TimelineCollectorMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (TimelineCollectorMetrics.class) {
        if (instance == null) {
          instance =
              DefaultMetricsSystem.initialize("TimelineService").register(
                  new TimelineCollectorMetrics());
          isInitialized.set(true);
        }
      }
    }
    return instance;
  }

  public synchronized static void destroy() {
    isInitialized.set(false);
    instance = null;
  }

  public MutableCounterInt getPutEntitiesTotalCount() {
    return putEntitiesTotalCount;
  }

  public MutableCounterInt getPutEntitiesSuccessCount() {
    return putEntitiesSuccessCount;
  }

  public MutableCounterInt getPutEntitiesFailureCount() {
    return putEntitiesFailureCount;
  }

  public MutableCounterInt getAsyncPutEntitiesTotalCount() {
    return asyncPutEntitiesTotalCount;
  }

  public MutableCounterInt getAsyncPutEntitiesSuccessCount() {
    return asyncPutEntitiesSuccessCount;
  }

  public MutableCounterInt getAsyncPutEntitiesFailureCount() {
    return asyncPutEntitiesFailureCount;
  }

  public void updatePutEntitiesMetrics(
      long startTimeNs, boolean succeeded, int incCount) {
    long durationMs = getElapsedTimeMs(startTimeNs);
    putEntitiesTotalCount.incr(incCount);
    putEntitiesLatency.add(durationMs);
    if (succeeded) {
      putEntitiesSuccessCount.incr(incCount);
      putEntitiesSuccessLatency.add(durationMs);
    } else {
      putEntitiesFailureCount.incr(incCount);
      putEntitiesFailureLatency.add(durationMs);
    }
  }

  public void updateAsyncPutEntitiesMetrics(
      long startTimeNs, boolean succeeded, int incCount) {
    long durationMs = getElapsedTimeMs(startTimeNs);
    asyncPutEntitiesTotalCount.incr(incCount);
    asyncPutEntitiesLatency.add(durationMs);
    if (succeeded) {
      asyncPutEntitiesSuccessCount.incr(incCount);
      asyncPutEntitiesSuccessLatency.add(durationMs);
    } else {
      asyncPutEntitiesFailureCount.incr(incCount);
      asyncPutEntitiesFailureLatency.add(durationMs);
    }
  }

  private long getElapsedTimeMs(long startTimeNs) {
    return TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNs,
        TimeUnit.NANOSECONDS);
  }
}
