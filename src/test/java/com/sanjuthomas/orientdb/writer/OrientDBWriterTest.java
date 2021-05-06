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

package com.sanjuthomas.orientdb.writer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.resolver.TombstoneResolver;
import com.sanjuthomas.orientdb.resolver.InsertWritableRecordResolver;
import com.sanjuthomas.orientdb.resolver.UpsertWritableRecordResolver;
import com.sanjuthomas.orientdb.writer.OrientDBConnectionBuilder.OrientDBConnection;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

/**
 * @author Sanju Thomas
 */
@ExtendWith(MockitoExtension.class)
public class OrientDBWriterTest {

  private OrientDBWriter orientDBWriter;
  @Mock
  private OrientDBConnection orientDBConnection;
  @Mock
  private ODatabaseDocument document;
  @Mock
  private OrientDBConnectionBuilder connectionBuilder;
  @Mock
  private OCommandRequest oCommandRequest;
  @Mock
  private ORecord oRecord;

  @BeforeEach
  void setUp() {
    when(connectionBuilder.build()).thenReturn(orientDBConnection);
    when(orientDBConnection.getDocument()).thenReturn(document);
    this.orientDBWriter = new OrientDBWriter(connectionBuilder);
  }

  @Test
  @ExtendWith(TombstoneResolver.class)
  void shouldDeleteDocumentFromDBWhenTombstoneMessageReceived(final WritableRecord writableRecord) {
    when(document.command(any(OCommandSQL.class))).thenReturn(oCommandRequest);
    orientDBWriter.write(Mono.just(List.of(writableRecord)));
    verify(document, times(1)).command(any(OCommandSQL.class));
  }

  @Test
  @ExtendWith(InsertWritableRecordResolver.class)
  void shouldInsert(final WritableRecord writableRecord) {
    when(document.save(any(ODocument.class))).thenReturn(oRecord);
    orientDBWriter.write(Mono.just(List.of(writableRecord)));
    verify(document, times(1)).save(any(ODocument.class));
  }

  @Test
  @ExtendWith(UpsertWritableRecordResolver.class)
  void shouldUpsert(final WritableRecord writableRecord) {
    when(document.command(any(OCommandSQL.class))).thenReturn(oCommandRequest);
    orientDBWriter.write(Mono.just(List.of(writableRecord)));
    verify(document, times(1)).command(any(OCommandSQL.class));
  }

  @Test
  @ExtendWith(InsertWritableRecordResolver.class)
  void shouldNotRetryWhenORecordDuplicatedExceptionIsThrown(final WritableRecord writableRecord) {
    when(document.save(any(ODocument.class))).thenThrow(new ORecordDuplicatedException("message", "indexName", new ORecordId(1, 1L), "key"));
    orientDBWriter.write(Mono.just(List.of(writableRecord)));
    verify(document, times(1)).save(any(ODocument.class));
  }


  @Test
  @ExtendWith(InsertWritableRecordResolver.class)
  void shouldRetryWhenGenericIsThrown(final WritableRecord writableRecord) {
    when(document.save(any(ODocument.class))).thenThrow(new RuntimeException("Test Exception"));
    orientDBWriter.write(Mono.just(List.of(writableRecord)));
    verify(document, times(2)).save(any(ODocument.class));
  }
}
