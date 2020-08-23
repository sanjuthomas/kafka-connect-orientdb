package com.sanjuthomas.orientdb.writer;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.sanjuthomas.orientdb.resolver.WritableRecordResolver;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteResult;
import com.sanjuthomas.orientdb.writer.OrientDBWriter;
import com.sanjuthomas.orientdb.writer.OrientDBWriter.Configuration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Mono;

/**
 * @author Sanju Thomas
 */
class OrientDBWriterTest {

  private OrientDBWriter orientDBWriter;

  @BeforeEach
  void setUp() {
    this.orientDBWriter = new OrientDBWriter(Configuration.builder()
      .type(ODatabaseType.MEMORY)
      .database("quote_request")
      .className("QuoteRequest")
      .connectionString("memory:/tmp")
      .username("admin")
      .password("admin")
      .build());
  }

  @AfterEach
  void tearDown() {
    orientDBWriter.close();
  }

  @Test
  @ExtendWith(WritableRecordResolver.class)
  void shouldWrite(final WritableRecord writableRecord) {
    final WriteResult writeResult = orientDBWriter.write(Mono.just(List.of(writableRecord))).block();
    Assertions.assertEquals(1, writeResult.getRecordsWritten());
    Assertions.assertEquals("QuoteRequest", writeResult.getClassName());
    Assertions.assertEquals(1, writeResult.getDocumentCount());
  }
}
