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

package com.sanjuthomas.orientdb.sink;

import com.sanjuthomas.orientdb.OrientDBSinkConfig;
import com.sanjuthomas.orientdb.OrientDBSinkConnector;
import com.sanjuthomas.orientdb.OrientDBSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.transform.SinkRecordTransformer;
import com.sanjuthomas.orientdb.writer.OrientDBWriter;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * @author Sanju Thomas
 */
@Slf4j
@Getter
public class OrientDBSinkTask extends SinkTask {

  @Setter
  private SinkRecordTransformer transformer;
  private OrientDBSinkResourceProvider resourceProvider;
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
    log.debug(
      "Received {} records. kafka coordinates from record: Topic - {}, Partition - {}, Offset - {}",
      recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

    final Map<String, List<WritableRecord>> groupedByTopic = transformer.apply(records);
    for(final Entry<String, List<WritableRecord>> entry : groupedByTopic.entrySet()) {
      final OrientDBWriter orientDBWriter = resourceProvider.writer(entry.getKey());
      try {
        orientDBWriter.write(entry.getValue());
      } catch (Exception exception) {
        log.error(exception.getMessage(), exception);
        resourceProvider.removeWriter(entry.getKey());
      }
    }
  }

  @Override
  public void start(final Map<String, String> config) {
    log.info("Original Configuration {}", config);
    log.info("Starting task for topics {}", config.get("topics"));
    retires = Integer.valueOf(Objects.requireNonNullElse(config.get("write.retries"), "2"));
    retryBackoffSeconds = Integer
      .valueOf(Objects.requireNonNullElse(config.get("retry.back.off.seconds"), "10"));
    final String topics = config.get(OrientDBSinkConfig.TOPICS);
    assert topics != null : String.format("%s %s", OrientDBSinkConfig.TOPICS, "is a required configuration");
    final String configFileLocation = config.get(OrientDBSinkConfig.CONFIG_FILE_LOCATION);
    assert configFileLocation != null : String.format("%s %s", OrientDBSinkConfig.CONFIG_FILE_LOCATION, "is a required configuration");
    log.info("Topics {} and config file location for the Topics {}", topics, configFileLocation);
    resourceProvider = OrientDBSinkResourceProvider.builder()
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
