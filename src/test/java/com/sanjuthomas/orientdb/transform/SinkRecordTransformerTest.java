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

import static org.mockito.Mockito.when;

import com.sanjuthomas.orientdb.OrientDBSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.resolver.SinkRecordsResolver;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * @author Sanju Thomas
 */
@ExtendWith(MockitoExtension.class)
class SinkRecordTransformerTest {

  private SinkRecordTransformer transformer;

  @Mock
  private OrientDBSinkResourceProvider provider;

  @BeforeEach
  void setUp() {
    this.transformer = new SinkRecordTransformer(provider);
  }

  @Test
  @ExtendWith(SinkRecordsResolver.class)
  void shouldTransform(final List<SinkRecord> sinkRecords) {
    when(provider.database("open_weather_data")).thenReturn("open_weather_data");
    when(provider.className("open_weather_data")).thenReturn("open_weather_data");
    when(provider.keyField("open_weather_data")).thenReturn("identifier");
    when(provider.database("quote_request")).thenReturn("open_weather_data");
    when(provider.className("quote_request")).thenReturn("open_weather_data");
    when(provider.keyField("quote_request")).thenReturn("identifier");
    final Map<String, List<WritableRecord>> grouped = transformer.apply(sinkRecords);

  }
}
