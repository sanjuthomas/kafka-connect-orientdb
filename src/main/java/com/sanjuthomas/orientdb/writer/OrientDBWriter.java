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

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteMode;
import com.sanjuthomas.orientdb.bean.WriteResult;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.errors.AlreadyExistsException;
import org.apache.kafka.connect.errors.RetriableException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDBWriter {

  @Getter
  private final Configuration configuration;
  private final ODatabaseDocument document;
  private final OrientDB db;

  /**
   * If the class does not exist, create the given class.
   *
   * @param configuration
   */
  public OrientDBWriter(final Configuration configuration) {
    this.configuration = configuration;
    db = new OrientDB(configuration.getConnectionString(),
      configuration.getUsername(), configuration.getPassword(),
      OrientDBConfig.defaultConfig());
    if (configuration.getType() == ODatabaseType.MEMORY) {
      db.createIfNotExists(configuration.getDatabase(), configuration.getType(),
        OrientDBConfig.defaultConfig());
    }
    document = db
      .open(configuration.getDatabase(), configuration.getUsername(), configuration.getPassword());
    document.createClassIfNotExist(configuration.getClassName());
  }

  @SuppressWarnings("deprecation")
  public Mono<WriteResult> write(final Mono<List<WritableRecord>> writableRecords) {
    return writableRecords
      .doOnNext(records -> {
        document.activateOnCurrentThread();
        document.begin();
        Flux.fromIterable(records)
          .doOnNext(record -> {
            if (null != record.getJsonDocumentString()) {
              if (WriteMode.UPSERT == record.getWriteMode()) {
                log.debug("UPSERT MODE: {}", record.upsertQuery());
                document.command(new OCommandSQL(record.upsertQuery())).execute();
              } else {
                log.debug("INSERT MODE: {}", record.getJsonDocumentString());
                document.save(
                  new ODocument(record.getClassName()).fromJSON(record.getJsonDocumentString()));
              }
            } else {
              log.debug("Tombstone message received to delete {} ", record.deleteQuery());
              document.command(new OCommandSQL(record.deleteQuery())).execute();
            }
          }).subscribe();
        document.commit();
      })
      .map(result -> WriteResult.builder()
        .className(result.get(0).getClassName())
        .recordsWritten(result.size())
        .documentCount(result.size()).build())
      .doOnError(err -> {
        document.rollback();
        if (err.getClass() == ORecordDuplicatedException.class) {
          throw new AlreadyExistsException(err.getMessage());
        }
        throw new RetriableException("Make another attempt, please.", err);
      })
      .doOnSuccess(result -> {
        log.debug("{} records written to database {} and class {}", result.getRecordsWritten(),
          configuration.getDatabase(), result.getClassName());
      });
  }

  public void close() {
    if (null != document) {
      document.close();
    }
    if (null != db) {
      db.close();
    }
  }

  @Builder
  @Getter
  public static class Configuration {

    private ODatabaseType type;
    private String connectionString;
    private String database;
    private String className;
    private String username;
    private String password;
    private List<String> suppressWriteExceptions;
  }
}
