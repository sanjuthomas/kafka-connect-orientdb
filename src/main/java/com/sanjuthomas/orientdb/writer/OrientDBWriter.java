package com.sanjuthomas.orientdb.writer;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteResult;
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
  final OrientDB db;
  private final ODatabaseDocument document;

  /**
   * If the database does not exist, try to create a new one.
   * If the class does not exist, create a new class.
   *
   * @param configuration
   */
  public OrientDBWriter(final Configuration configuration) {
    this.configuration = configuration;
    db = new OrientDB(configuration.getConnectionString(),
      configuration.getUsername(), configuration.getPassword(),
      OrientDBConfig.defaultConfig());
    final boolean result = db
      .createIfNotExists(configuration.getDatabase(), configuration.getType(),
        OrientDBConfig.defaultConfig());
    if (result) {
      log.info("{} database is created.", configuration.getDatabase());
    }
    document = db
      .open(configuration.getDatabase(), configuration.getUsername(), configuration.getPassword());
    document.createClassIfNotExist(configuration.getClassName());
  }

  public Mono<WriteResult> write(final Mono<List<WritableRecord>> writableRecords) {
    return writableRecords
      .doOnNext(records -> {
        document.begin();
        Flux.fromIterable(records)
          .doOnNext(record -> {
            document.save(new ODocument(record.getClassName()).fromJSON(record.getJsonDocumentString()));
          })
          .subscribe();
        document.commit();
      })
      .map(result -> WriteResult.builder()
        .className(result.get(0).getClassName())
        .recordsWritten(result.size())
        .documentCount(result.size()).build())
      .doOnError(err -> {
        log.error(err.getMessage(), err);
        document.rollback();
        throw new RetriableException("Make another attempt, please.");
      })
      .doOnSuccess(result -> {
        log.info("{} records written to database {} and class {}", result.getRecordsWritten(),
          configuration.getDatabase(), result.getClassName());
      });
  }

  public void close() {
    if(null != document)
    document.close();
    if(null != db)
    db.close();
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
