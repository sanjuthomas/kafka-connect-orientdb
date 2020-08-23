package com.sanjuthomas.orientdb.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.sanjuthomas.orientdb.resolver.SinkRecordsResolver;
import com.sanjuthomas.orientdb.OrientDbSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.transform.SinkRecordTransformer;
import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.test.StepVerifier;

/**
 * @author Sanju Thomas
 */
@ExtendWith(MockitoExtension.class)
class SinkRecordTransformerTest {

  private SinkRecordTransformer transformer;

  @Mock
  private OrientDbSinkResourceProvider provider;

  @BeforeEach
  void setUp() {
    this.transformer = new SinkRecordTransformer(provider);
  }

  @Test
  @ExtendWith(SinkRecordsResolver.class)
  void shouldTransform(final List<SinkRecord> sinkRecords) {
    when(provider.database("open_weather_data")).thenReturn("open_weather_data");
    when(provider.database("quote_request")).thenReturn("quote_request");
    when(provider.className("open_weather_data")).thenReturn("open_weather_data");
    when(provider.className("quote_request")).thenReturn("quote_request");
    final Flux<GroupedFlux<String, WritableRecord>> groupedFlux = transformer
      .apply(Flux.fromIterable(sinkRecords));
    StepVerifier.create(groupedFlux)
      .assertNext(r -> {
          assertThat(r.key()).isEqualTo("open_weather_data");
          StepVerifier.create(r)
            .assertNext(t -> assertThat(t.getTopic()).isEqualTo("open_weather_data"));
        }
      )
      .assertNext(r -> {
          assertThat(r.key()).isEqualTo("quote_request");
          StepVerifier.create(r)
            .assertNext(t -> assertThat(t.getTopic()).isEqualTo("quote_request"));
        }
      )
      .expectComplete()
      .verify();
  }
}
