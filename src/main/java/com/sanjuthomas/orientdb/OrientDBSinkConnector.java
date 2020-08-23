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

package com.sanjuthomas.orientdb;

import com.sanjuthomas.orientdb.sink.OrientDBSinkTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDBSinkConnector extends SinkConnector {

  public static final String ORIENTDB_CONNECTOR_VERSION = "1.0";

  private Map<String, String> config;

  @Override
  public ConfigDef config() {
    return OrientDBSinkConfig.CONFIG_DEF;
  }

  @Override
  public void start(final Map<String, String> arg0) {
    config = arg0;
  }

  @Override
  public void stop() {
    log.info("stop called");
  }

  @Override
  public Class<? extends Task> taskClass() {
    return OrientDBSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(final int taskCunt) {
    final List<Map<String, String>> configs = new ArrayList<>(taskCunt);
    for (int i = 0; i < taskCunt; ++i) {
      configs.add(config);
    }
    return configs;
  }

  @Override
  public String version() {
    return ORIENTDB_CONNECTOR_VERSION;
  }
}
