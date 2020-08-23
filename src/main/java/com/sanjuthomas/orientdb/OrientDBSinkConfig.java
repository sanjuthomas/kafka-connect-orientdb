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
  public static final String CONFIG_FILE_LOCATION = "databaseConfigFileLocation";

  public static ConfigDef CONFIG_DEF = new ConfigDef();

  public OrientDBSinkConfig(final Map<?, ?> originals) {

    super(CONFIG_DEF, originals, false);
    log.info("Original Configs {}", originals);
  }
}