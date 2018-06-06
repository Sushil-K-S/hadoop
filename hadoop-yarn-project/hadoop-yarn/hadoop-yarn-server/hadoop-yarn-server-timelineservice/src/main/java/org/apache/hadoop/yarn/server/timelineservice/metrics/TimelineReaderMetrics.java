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
 * Metrics class for TimelineReader.
 */
@Metrics(about = "Metrics for timeline reader", context = "timelineservice")
public class TimelineReaderMetrics {

  private final static MetricsInfo METRICS_INFO = info("TimelineReaderMetrics",
      "Metrics for TimelineReader");
  private static AtomicBoolean isInitialized = new AtomicBoolean(false);
  private static volatile TimelineReaderMetrics instance = null;
  private final MetricsRegistry registry;
  @Metric("GET timeline entities total count")
  private MutableCounterInt getEntitiesTotalCount;
  @Metric("GET timeline entities success count")
  private MutableCounterInt getEntitiesSuccessCount;
  @Metric("GET timeline entities failure count")
  private MutableCounterInt getEntitiesFailureCount;
  @Metric("GET entity types total count")
  private MutableCounterInt getEntityTypesTotalCount;
  @Metric("GET entity types success count")
  private MutableCounterInt getEntityTypesSuccessCount;
  @Metric("GET entity types failure count")
  private MutableCounterInt getEntityTypesFailureCount;

  private MutableQuantiles getEntitiesLatency;
  private MutableQuantiles getEntitiesSuccessLatency;
  private MutableQuantiles getEntitiesFailureLatency;
  private MutableQuantiles getEntityTypesLatency;
  private MutableQuantiles getEntityTypesSuccessLatency;
  private MutableQuantiles getEntityTypesFailureLatency;

  private TimelineReaderMetrics() {
    this.registry = new MetricsRegistry(METRICS_INFO);
    this.getEntitiesLatency =
        registry.newQuantiles("getEntitiesLatency",
            "GET entities latency", "ops", "latency", 10);
    this.getEntitiesSuccessLatency =
        registry.newQuantiles("getEntitiesSuccessLatency",
            "GET entities success latency", "ops", "latency", 10);
    this.getEntitiesFailureLatency =
        registry.newQuantiles("getEntitiesFailureLatency",
            "GET entities failure latency", "ops", "latency", 10);

    this.getEntityTypesLatency =
        registry.newQuantiles("getEntityTypesLatency",
            "GET entity types latency", "ops", "latency", 10);
    this.getEntityTypesSuccessLatency =
        registry.newQuantiles("getEntityTypesSuccessLatency",
            "GET entity types success latency", "ops", "latency", 10);
    this.getEntityTypesFailureLatency =
        registry.newQuantiles("getEntityTypesFailureLatency",
            "GET entity types failure latency", "ops", "latency", 10);
  }

  public static TimelineReaderMetrics getInstance() {
    if (!isInitialized.get()) {
      synchronized (TimelineReaderMetrics.class) {
        if (instance == null) {
          instance =
              DefaultMetricsSystem.initialize("TimelineService").register(
                  new TimelineReaderMetrics());
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

  public MutableCounterInt getEntitiesTotalCount() {
    return getEntitiesTotalCount;
  }

  public MutableCounterInt getEntitiesSuccessCount() {
    return getEntitiesSuccessCount;
  }

  public MutableCounterInt getEntitiesFailureCount() {
    return getEntitiesFailureCount;
  }

  public MutableCounterInt getEntityTypesTotalCount() {
    return getEntityTypesTotalCount;
  }

  public MutableCounterInt getEntityTypesSuccessCount() {
    return getEntityTypesSuccessCount;
  }

  public MutableCounterInt getEntityTypesFailureCount() {
    return getEntityTypesFailureCount;
  }

  public void updateGetEntitiesMetrics(
      long startTimeNs, boolean succeeded, int incCount) {
    long durationMs = getElapsedTimeMs(startTimeNs);
    getEntitiesTotalCount.incr(incCount);
    getEntitiesLatency.add(durationMs);
    if (succeeded) {
      getEntitiesSuccessCount.incr(incCount);
      getEntitiesSuccessLatency.add(durationMs);
    } else {
      getEntitiesFailureCount.incr(incCount);
      getEntitiesFailureLatency.add(durationMs);
    }
  }

  public void updateGetEntityTypesMetrics(
      long startTimeNs, boolean succeeded, int incCount) {
    long durationMs = getElapsedTimeMs(startTimeNs);
    getEntityTypesTotalCount.incr(incCount);
    getEntityTypesLatency.add(durationMs);
    if (succeeded) {
      getEntityTypesSuccessCount.incr(incCount);
      getEntityTypesSuccessLatency.add(durationMs);
    } else {
      getEntityTypesFailureCount.incr(incCount);
      getEntityTypesFailureLatency.add(durationMs);
    }
  }

  private long getElapsedTimeMs(long startTimeNs) {
    return TimeUnit.MILLISECONDS.convert(System.nanoTime() - startTimeNs,
        TimeUnit.NANOSECONDS);
  }
}
