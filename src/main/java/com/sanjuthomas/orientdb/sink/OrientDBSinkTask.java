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

package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.OrientDBSinkConfig;
import com.sanjuthomas.orientdb.OrientDBSinkConnector;
import com.sanjuthomas.orientdb.OrientDbSinkResourceProvider;
import com.sanjuthomas.orientdb.transform.SinkRecordTransformer;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;
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
@Getter
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
    retires = Integer.valueOf(Objects.requireNonNullElse(config.get("write.retries"), "2"));
    retryBackoffSeconds = Integer
      .valueOf(Objects.requireNonNullElse(config.get("retry.back.off.seconds"), "10"));
    final String topics = config.get(OrientDBSinkConfig.TOPICS);
    assert topics != null : "topics is a required configuration";
    final String configFileLocation = config.get(OrientDBSinkConfig.CONFIG_FILE_LOCATION);
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
