/*
 *  Copyright (c) 2020 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  You may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.
 *
 *  See the License for the specific language governing permissions
 *  and limitations under the License.
 */

package com.sanjuthomas.orientdb.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteResult;
import com.sanjuthomas.orientdb.resolver.WritableRecordResolver;
import com.sanjuthomas.orientdb.writer.OrientDBWriter.Configuration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
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
    final WriteResult writeResult = orientDBWriter.write(Mono.just(List.of(writableRecord)))
      .block();
    assertEquals(1, writeResult.getRecordsWritten());
    assertEquals("QuoteRequest", writeResult.getClassName());
    assertEquals(1, writeResult.getDocumentCount());
  }

}
