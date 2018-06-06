package org.apache.hadoop.yarn.server.timelineservice.reader;

import org.apache.hadoop.yarn.server.timelineservice.metrics.TimelineReaderMetrics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestTimelineReaderMetrics {

  private TimelineReaderMetrics metrics;

  /**
   * Test Timeline Reader Metrics Metrics
   */
  @Test
  public void testTimelineReaderMetrics() {
    Assert.assertNotNull(metrics);
    Assert.assertEquals(0, metrics.getEntitiesTotalCount().value());
    Assert.assertEquals(0, metrics.getEntitiesSuccessCount().value());
    Assert.assertEquals(0, metrics.getEntitiesFailureCount().value());
    metrics.updateGetEntitiesMetrics(0, true, 1);
    Assert.assertEquals(1, metrics.getEntitiesTotalCount().value());
    Assert.assertEquals(1, metrics.getEntitiesSuccessCount().value());
    Assert.assertEquals(0, metrics.getEntitiesFailureCount().value());

    Assert.assertEquals(0, metrics.getEntityTypesTotalCount().value());
    Assert.assertEquals(0, metrics.getEntityTypesSuccessCount().value());
    Assert.assertEquals(0, metrics.getEntityTypesFailureCount().value());
    metrics.updateGetEntityTypesMetrics(0, false, 1);
    Assert.assertEquals(1, metrics.getEntityTypesTotalCount().value());
    Assert.assertEquals(0, metrics.getEntityTypesSuccessCount().value());
    Assert.assertEquals(1, metrics.getEntityTypesFailureCount().value());
  }

  @Before
  public void setup() {
    metrics = TimelineReaderMetrics.getInstance();
  }

  @After
  public void tearDown() {
    TimelineReaderMetrics.destroy();
  }
}
