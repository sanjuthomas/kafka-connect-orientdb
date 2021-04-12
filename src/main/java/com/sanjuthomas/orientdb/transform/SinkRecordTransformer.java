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

package com.sanjuthomas.orientdb.transform;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.orientdb.OrientDbSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import java.rmi.server.UID;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.connect.sink.SinkRecord;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

/**
 * @author Sanju Thomas
 */

@RequiredArgsConstructor
public class SinkRecordTransformer implements
  Function<Flux<SinkRecord>, Flux<GroupedFlux<String, WritableRecord>>> {

  private final TypeReference<Map<String, Object>> typeReference = new TypeReference<>() {};
  private final ObjectMapper MAPPER = new ObjectMapper();
  private final OrientDbSinkResourceProvider provider;

  @Override
  public Flux<GroupedFlux<String, WritableRecord>> apply(Flux<SinkRecord> records) {
    return records.flatMap(record -> Flux.just(WritableRecord.builder()
      .topic(record.topic())
      .database(provider.database(record.topic()))
      .className(provider.className(record.topic()))
      .keyField(provider.keyField(record.topic()))
      .keyValue(keyValue(record))
      .jsonDocumentString(toJson(record))
      .build()))
      .groupBy(writableRecord -> writableRecord.getTopic());
  }

  @SneakyThrows
  private String toJson(final SinkRecord record) {
    final Object value = record.value();
    if(null != value) {
      final Map<String, Object> payload = MAPPER.convertValue(value, typeReference);
      payload.put(provider.keyField(record.topic()), keyValue(record));
      return MAPPER.writeValueAsString(payload);
    }
    return null;
  }

  private String keyValue(final SinkRecord record) {
    final Object key = record.key();
    return null != key ? (String) key : UUID.randomUUID().toString();
  }

}
