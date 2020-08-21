package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.sink.writer.OrientDBWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Sanju Thomas
 *
 */
class OrientDbSinkResourceProviderTest {


  @Test
  void shouldLoadConfig() {
    final OrientDbSinkResourceProvider config = OrientDbSinkResourceProvider.builder()
      .using(new String[]{"quote_request"}, "config").build();
    Assertions.assertEquals("quote_request", config.database("quote_request"));
    Assertions.assertEquals("QuoteRequest", config.className("quote_request"));
    Assertions.assertEquals("OrientDBWriter", config.writer("quote_request").getClass().getSimpleName());
    final OrientDBWriter orientDBWriter = config.writer("quote_request");
    Assertions.assertEquals(orientDBWriter.hashCode(), config.writer("quote_request").hashCode());
    config.removeWriter("quote_request");
    Assertions.assertNotEquals(orientDBWriter.hashCode(), config.writer("quote_request").hashCode());
    final OrientDBWriter writer = config.writer("quote_request");
  }


  @Test
  void shouldBuildWriter() {
    final OrientDbSinkResourceProvider config = OrientDbSinkResourceProvider.builder()
      .using(new String[]{"open_weather_data"}, "config").build();
    final OrientDBWriter orientDBWriter = config.writer("open_weather_data");
    Assertions.assertEquals("open_weather_data", orientDBWriter.getConfiguration().getDatabase());
    Assertions.assertEquals("remote:localhost", orientDBWriter.getConfiguration().getConnectionString());
    Assertions.assertEquals("admin", orientDBWriter.getConfiguration().getUsername());
  }
}
