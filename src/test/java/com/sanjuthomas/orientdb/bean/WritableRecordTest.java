/*
 * Copyright (c) 2021 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

package com.sanjuthomas.orientdb.bean;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Sanju Thomas
 */
public class WritableRecordTest {

  private WritableRecord writableRecordWithStringKey;
  private WritableRecord writableRecordWithNumberKey;

  @BeforeEach
  void setUp() {
    writableRecordWithStringKey = WritableRecord.builder().database("database")
      .className("className")
      .keyField("key")
      .keyValue("value")
      .jsonDocumentString("{\"name\" : \"Joe Doe\"}")
      .build();

    writableRecordWithNumberKey = WritableRecord.builder().database("database")
      .className("className")
      .keyField("key")
      .keyValue(1L)
      .jsonDocumentString("{\"name\" : \"Joe Doe\"}")
      .build();
  }

  @Test
  void shouldBuildUpsertQuery() {
    Assertions
      .assertEquals("UPDATE className MERGE {\"name\" : \"Joe Doe\"} UPSERT WHERE key = 'value'",
        writableRecordWithStringKey.upsertQuery());
    Assertions.assertEquals("UPDATE className MERGE {\"name\" : \"Joe Doe\"} UPSERT WHERE key = 1",
      writableRecordWithNumberKey.upsertQuery());
  }

  @Test
  void shouldBuildDeleteQuery() {
    Assertions.assertEquals("DELETE from className WHERE key = 'value'",
      writableRecordWithStringKey.deleteQuery());
    Assertions.assertEquals("DELETE from className WHERE key = 1",
      writableRecordWithNumberKey.deleteQuery());
  }
}
