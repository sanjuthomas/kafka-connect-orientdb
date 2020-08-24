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

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDBSinkConfig extends AbstractConfig {

  public static final String TOPICS = "topics";
  public static final String CONFIG_FILE_LOCATION = "databaseConfigFilesLocation";

  public static ConfigDef CONFIG_DEF = new ConfigDef();

  public OrientDBSinkConfig(final Map<?, ?> originals) {

    super(CONFIG_DEF, originals, false);
    log.info("Original Configs {}", originals);
  }
}