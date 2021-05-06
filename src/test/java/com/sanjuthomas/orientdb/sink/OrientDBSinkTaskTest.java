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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.sanjuthomas.orientdb.OrientDBSinkResourceProvider;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.resolver.SinkConnectorConfigResolver;
import com.sanjuthomas.orientdb.resolver.SinkRecordsResolver;
import com.sanjuthomas.orientdb.transform.SinkRecordTransformer;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;

/**
 * @author Sanju Thomas
 */
@ExtendWith(MockitoExtension.class)
class OrientDBSinkTaskTest {

  private OrientDBSinkTask orientDBSinkTask;

  @Mock
  private OrientDBSinkResourceProvider resourceProvider;

  @BeforeEach
  void setUp() {
    orientDBSinkTask = new OrientDBSinkTask();
    when(resourceProvider.keyField(anyString())).thenReturn("id");
  }

  @Test
  @ExtendWith({SinkRecordsResolver.class, SinkConnectorConfigResolver.class})
  void shouldRetry(final List<SinkRecord> sinkRecords, final Map<String, String> config) {
    orientDBSinkTask.start(config);
    orientDBSinkTask.setTransformer(new SinkRecordTransformer(resourceProvider));
    orientDBSinkTask.put(sinkRecords);
  }
}
