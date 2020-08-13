package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.sink.writer.OrientDBWriter;
import org.junit.jupiter.api.Test;

/**
 * @author Sanju Thomas
 */
class OrientDbSinkResourceProviderTest {


  @Test
  void shouldBuild() {
    final OrientDbSinkResourceProvider config = OrientDbSinkResourceProvider.builder()
      .using(new String[]{"quote_request"}, "config").build();
      final OrientDBWriter writer = config.writer("quote_request");
    System.out.println(writer);

  }

}
