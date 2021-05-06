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

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteMode;
import com.sanjuthomas.orientdb.writer.OrientDBConnectionBuilder.OrientDBConnection;
import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Sanju Thomas
 */
@Slf4j
@RequiredArgsConstructor
public class WriteOperations implements Function<List<WritableRecord>, Void> {

  private final OrientDBConnection orientDBConnection;

  @Override
  public Void apply(List<WritableRecord> writableRecords) {
    orientDBConnection.getDocument().activateOnCurrentThread();
    orientDBConnection.getDocument().begin();
    for (final WritableRecord record : writableRecords) {
      if (null != record.getJsonDocumentString()) {
        if (null != record.getJsonDocumentString()) {
          if (WriteMode.UPSERT == record.getWriteMode()) {
            log.debug("UPSERT MODE: {}", record.upsertQuery());
            update(record);
          } else {
            log.debug("INSERT MODE: {}", record.getJsonDocumentString());
            save(record);
          }
        }
      } else {
        log.debug("Tombstone message received to delete {} ", record.deleteQuery());
        delete(record);
      }
    }
    orientDBConnection.getDocument().commit();
    return null;
  }


  void save(final WritableRecord record) {
    try {
      orientDBConnection.getDocument().save(
        new ODocument(record.getClassName()).fromJSON(record.getJsonDocumentString()));
    } catch (ORecordDuplicatedException e) {
      log.warn(e.getMessage());
    }
  }

  @SuppressWarnings("deprecation")
  void update(final WritableRecord record) {
    orientDBConnection.getDocument().command(new OCommandSQL(record.upsertQuery())).execute();
  }

  @SuppressWarnings("deprecation")
  void delete(final WritableRecord record) {
    orientDBConnection.getDocument().command(new OCommandSQL(record.deleteQuery())).execute();
  }

}
