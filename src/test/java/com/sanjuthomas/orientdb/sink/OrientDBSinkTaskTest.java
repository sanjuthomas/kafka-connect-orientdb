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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.sanjuthomas.orientdb.bean.WriteMode;
import com.sanjuthomas.orientdb.resolver.SinkConnectorConfigResolver;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * @author Sanju Thomas
 */
class OrientDBSinkTaskTest {

  private OrientDBSinkTask orientDBSinkTask;

  @BeforeEach
  void setUp() {
    orientDBSinkTask = new OrientDBSinkTask();
  }

  @AfterEach
  void tearDown() {
    orientDBSinkTask.getResourceProvider().writer("open_weather_data").close();
  }

  @Test
  @ExtendWith(SinkConnectorConfigResolver.class)
  void shouldStart(final Map<String, String> config) {
    orientDBSinkTask.start(config);
    assertEquals(2, orientDBSinkTask.getRetires());
    assertEquals(10, orientDBSinkTask.getRetryBackoffSeconds());
    assertEquals("SinkRecordTransformer",
      orientDBSinkTask.getTransformer().getClass().getSimpleName());
    assertEquals("open_weather_data",
      orientDBSinkTask.getResourceProvider().writer("open_weather_data").getConfiguration()
        .getDatabase());
    assertEquals("memory:/tmp",
      orientDBSinkTask.getResourceProvider().writer("open_weather_data").getConfiguration()
        .getConnectionString());
    assertEquals(WriteMode.UPSERT, orientDBSinkTask.getResourceProvider().writeMode("open_weather_data"));
    assertEquals(WriteMode.INSERT, orientDBSinkTask.getResourceProvider().writeMode("quote_request"));
  }

}
