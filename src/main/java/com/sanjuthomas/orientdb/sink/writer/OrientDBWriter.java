package com.sanjuthomas.orientdb.sink.writer;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.sanjuthomas.orientdb.sink.bean.WritableRecord;
import com.sanjuthomas.orientdb.sink.bean.WriteResult;
import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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
  private final ODatabaseDocument db;

  /**
   * If the database does not exist, try to create a new one.
   * If the class does not exist, create a new class.
   *
   * @param configuration
   */
  public OrientDBWriter(final Configuration configuration) {
    this.configuration = configuration;
    final OrientDB orientDB = new OrientDB(configuration.getConnectionString(),
      configuration.getUsername(), configuration.getPassword(),
      OrientDBConfig.defaultConfig());
    final boolean result = orientDB
      .createIfNotExists(configuration.getDatabase(), configuration.getType(),
        OrientDBConfig.defaultConfig());
    if (result) {
      log.info("{} database is created.", configuration.getDatabase());
    }
    db = orientDB
      .open(configuration.getDatabase(), configuration.getUsername(), configuration.getPassword());
    db.createClassIfNotExist(configuration.getClassName());
  }

  public Mono<WriteResult> write(final Mono<List<WritableRecord>> writableRecords) {
    return writableRecords
      .doOnNext(records -> {
        db.begin();
        Flux.fromIterable(records)
          .doOnNext(record -> {
            db.save(new ODocument(record.getClassName()).fromJSON(record.getJsonDocumentString()));
          })
          .subscribe();
        db.commit();
      })
      .map(result -> WriteResult.builder()
        .className(result.get(0).getClassName())
        .recordsWritten(result.size())
        .documentCount(result.size()).build())
      .doOnError(err -> {
        log.error(err.getMessage(), err);
        db.rollback();
        throw new RetriableException("Make another attempt, please.");
      })
      .doOnSuccess(result -> {
        log.info("{} records written to database {} and class {}", result.getRecordsWritten(),
          configuration.getDatabase(), result.getClassName());
      });
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
  }
}
