package com.sanjuthomas.orientdb.sink.transform;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.orientdb.sink.OrientDbSinkResourceProvider;
import com.sanjuthomas.orientdb.sink.bean.WritableRecord;
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

  private final ObjectMapper MAPPER = new ObjectMapper();
  private final OrientDbSinkResourceProvider provider;

  @Override
  public Flux<GroupedFlux<String, WritableRecord>> apply(Flux<SinkRecord> record) {
    return record.flatMap(r -> Flux.just(WritableRecord.builder()
      .topic(r.topic())
      .database(provider.database(r.topic()))
      .className(provider.className(r.topic()))
      .jsonDocumentString(toJson(r.value()))
      .build()))
      .groupBy(writableRecord -> writableRecord.getTopic());
  }

  @SneakyThrows
  private String toJson(final Object value) {
    return MAPPER.writeValueAsString(value);
  }
}
