package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.sink.transform.SinkRecordTransformer;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDBSinkTask extends SinkTask {

  private SinkRecordTransformer transformer;
  private OrientDbSinkResourceProvider resourceProvider;
  private Integer retires;
  private Integer retryBackoffSeconds;

  @Override
  public void put(final Collection<SinkRecord> records) {

    if (records.isEmpty()) {
      log.debug("Empty record collection to process");
      return;
    }

    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.info(
      "Received {} records. kafka coordinates from record: Topic - {}, Partition - {}, Offset - {}",
      recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    Flux.fromIterable(records)
      .transform(transformer)
      .flatMap(grouped -> resourceProvider.writer(grouped.key()).write(grouped.collectList()))
      .retryWhen(Retry.backoff(retires, Duration.ofSeconds(retryBackoffSeconds))
        .filter(e -> e.getClass() == RetriableException.class))
      .doOnError(err -> {
        log.error(err.getMessage(), err);
        throw new ConnectException(
          "Retries exhausted, ending the task. Manual restart is required.");
      })
      .blockLast();
  }

  @Override
  public void start(final Map<String, String> config) {

    log.info("task {} started with config {}", Thread.currentThread().getId(), config);
    retires = (Integer) Objects.requireNonNullElse(config.get("write.retries"), 2);
    retryBackoffSeconds = (Integer) Objects.requireNonNullElse(config.get("retry.back.off.seconds"), 10);
    final String topics = config.get(OrientDBSinkConfig.TOPICS);
    final String configFileLocation = config.get(OrientDBSinkConfig.CONFIG_FILE_LOCATION);
    assert topics != null : "topics is a required configuration";
    assert configFileLocation != null : "databaseConfigFileLocation is a required configuration";
    resourceProvider = OrientDbSinkResourceProvider.builder()
      .using(topics.split(","), configFileLocation)
      .build();
    transformer = new SinkRecordTransformer(resourceProvider);
    log.info("Config initialization completed");
  }

  @Override
  public void stop() {
    log.info("stop called!");
  }

  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    currentOffsets
      .forEach((k, v) -> log.debug("Flush - Topic {}, Partition {}, Offset {}, Metadata {}",
        k.topic(), k.partition(), v.offset(), v.metadata()));
  }

  public String version() {
    return OrientDBSinkConnector.ORIENTDB_CONNECTOR_VERSION;
  }
}
