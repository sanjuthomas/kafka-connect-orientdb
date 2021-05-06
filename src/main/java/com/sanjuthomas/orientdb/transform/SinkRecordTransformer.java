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

package com.sanjuthomas.orientdb.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.orientdb.OrientDBSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * @author Sanju Thomas
 */

@RequiredArgsConstructor
public class SinkRecordTransformer implements Function<Collection<SinkRecord>, Map<String, List<WritableRecord>>> {

  private final TypeReference<Map<String, Object>> typeReference = new TypeReference<>() {
  };
  private final ObjectMapper MAPPER = new ObjectMapper();
  private final OrientDBSinkResourceProvider provider;

  @Override
  public Map<String, List<WritableRecord>> apply(Collection<SinkRecord> sinkRecords) {

    final Map<String, List<WritableRecord>> grouped = new HashMap<>();
    for(final SinkRecord sinkRecord : sinkRecords) {
      grouped.computeIfAbsent(sinkRecord.topic(), value -> new ArrayList<>())
        .add(WritableRecord.builder()
        .topic(sinkRecord.topic())
        .database(provider.database(sinkRecord.topic()))
        .className(provider.className(sinkRecord.topic()))
        .keyField(provider.keyField(sinkRecord.topic()))
        .writeMode(provider.writeMode(sinkRecord.topic()))
        .keyValue(keyValue(sinkRecord))
        .jsonDocumentString(toJson(sinkRecord))
        .build());
    }
    return grouped;
  }

  @SneakyThrows
  private String toJson(final SinkRecord record) {
    final Object value = record.value();
    if (null != value) {
      final Map<String, Object> payload = MAPPER.convertValue(value, typeReference);
      payload.put(provider.keyField(record.topic()), keyValue(record));
      return MAPPER.writeValueAsString(payload);
    }
    return null;
  }

  private Object keyValue(final SinkRecord record) {
    final Object key = record.key();
    return null != key ? key : UUID.randomUUID().toString();
  }

}
