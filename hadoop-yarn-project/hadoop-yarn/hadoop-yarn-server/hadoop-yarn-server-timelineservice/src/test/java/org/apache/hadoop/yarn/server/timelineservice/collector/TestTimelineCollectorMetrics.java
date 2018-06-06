package org.apache.hadoop.yarn.server.timelineservice.collector;

import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineCollectorMetrics;
import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTimelineCollectorMetrics {

  private TimelineCollectorMetrics metrics;

  /**
   * Test Timeline Reader Metrics Metrics
   */
  @Test
  public void testTimelineCollectorMetrics() {
    Assert.assertNotNull(metrics);
    Assert.assertEquals(0, metrics.getPutEntitiesTotalCount().value());
    Assert.assertEquals(0, metrics.getPutEntitiesSuccessCount().value());
    Assert.assertEquals(0, metrics.getPutEntitiesFailureCount().value());
    metrics.updatePutEntitiesMetrics(0, true, 1);
    Assert.assertEquals(1, metrics.getPutEntitiesTotalCount().value());
    Assert.assertEquals(1, metrics.getPutEntitiesSuccessCount().value());
    Assert.assertEquals(0, metrics.getPutEntitiesFailureCount().value());

    Assert.assertEquals(0, metrics.getAsyncPutEntitiesTotalCount().value());
    Assert.assertEquals(0, metrics.getAsyncPutEntitiesSuccessCount().value());
    Assert.assertEquals(0, metrics.getAsyncPutEntitiesFailureCount().value());
    metrics.updateAsyncPutEntitiesMetrics(0, false, 1);
    Assert.assertEquals(1, metrics.getAsyncPutEntitiesTotalCount().value());
    Assert.assertEquals(0, metrics.getAsyncPutEntitiesSuccessCount().value());
    Assert.assertEquals(1, metrics.getAsyncPutEntitiesFailureCount().value());
  }

  @Before
  public void setup() {
    metrics = TimelineCollectorMetrics.getInstance();
  }

  @After
  public void tearDown() {
    TimelineReaderMetrics.destroy();
  }
}
