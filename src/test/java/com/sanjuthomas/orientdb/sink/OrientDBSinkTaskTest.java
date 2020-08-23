package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.resolver.SinkConnectorConfigResolver;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author Sanju Thomas
 */
class OrientDBSinkTaskTest {

  private OrientDBSinkTask orientDBSinkTask;

  @BeforeEach
  void setUp() {
    orientDBSinkTask = new OrientDBSinkTask();
  }

  @AfterEach
  void tearDown() {
    orientDBSinkTask.getResourceProvider().writer("open_weather_data").close();
  }

  @Test
  @ExtendWith(SinkConnectorConfigResolver.class)
  void shouldStart(final Map<String, String> config) {
    orientDBSinkTask.start(config);
    Assertions.assertEquals(2, orientDBSinkTask.getRetires());
    Assertions.assertEquals(10, orientDBSinkTask.getRetryBackoffSeconds());
    Assertions.assertEquals("SinkRecordTransformer", orientDBSinkTask.getTransformer().getClass().getSimpleName());
    Assertions.assertEquals("open_weather_data", orientDBSinkTask.getResourceProvider().writer("open_weather_data").getConfiguration().getDatabase());
    Assertions.assertEquals("memory:/tmp", orientDBSinkTask.getResourceProvider().writer("open_weather_data").getConfiguration().getConnectionString());
  }
}
