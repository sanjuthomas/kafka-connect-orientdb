package com.sanjuthomas.orientdb.bean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Sanju Thomas
 */
public class WritableRecordTest {

  private WritableRecord writableRecord;

  @BeforeEach
  void setUp() {
    writableRecord = WritableRecord.builder().database("database")
      .className("className")
      .keyField("key")
      .keyValue("value")
      .jsonDocumentString("{\"name\" : \"Joe Doe\"}")
      .build();
  }

  @Test
  void shouldBuildUpsertQuery() {
    Assertions.assertEquals("UPDATE className MERGE {\"name\" : \"Joe Doe\"} UPSERT WHERE key = 'value'", writableRecord.upsertQuery());
  }

  @Test
  void shouldBuildDeleteQuery() {
    Assertions.assertEquals("DELETE from className WHERE key = 'value'", writableRecord.deleteQuery());
  }
}
