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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.sanjuthomas.orientdb.writer.OrientDBWriter;
import com.sanjuthomas.orientdb.writer.OrientDBWriter.Configuration;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDbSinkResourceProvider {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private final Map<String, Config> configMap;
  private final Map<String, OrientDBWriter> writerMap = new ConcurrentHashMap<>();

  private OrientDbSinkResourceProvider(final Map<String, Config> configMap) {
    this.configMap = configMap;
  }

  public static Builder builder() {
    return new Builder();
  }

  public synchronized OrientDBWriter writer(final String topic) {
    if (writerMap.containsKey(topic)) {
      return writerMap.get(topic);
    }

    if (!configMap.containsKey(topic)) {
      throw new IllegalStateException(String.format("No configuration found for topic %s", topic));
    }

    final Config config = configMap.get(topic);
    final OrientDBWriter orientDBWriter = new OrientDBWriter(Configuration.builder()
      .type(config.getConnectionString().toLowerCase().startsWith("memory:")? ODatabaseType.MEMORY : ODatabaseType.PLOCAL)
      .connectionString(config.getConnectionString())
      .database(config.getDatabase())
      .className(config.getClassName())
      .username(config.getUsername())
      .password(config.getPassword())
      .build());
    writerMap.put(topic, orientDBWriter);
    return orientDBWriter;
  }

  public synchronized void removeWriter(final String topic) {
    if (writerMap.containsKey(topic)) {
      writerMap.get(topic).close();
      writerMap.remove(topic);
    }
  }

  public String className(final String topic) {
    return configMap.get(topic).getClassName();
  }

  public String database(final String topic) {
    return configMap.get(topic).getDatabase();
  }

  public static class Builder {

    private Map<String, Config> topicToClassNameMapping;

    public Builder using(final String[] topics,
      final String databaseConfigFilesLocation) {

      final List<Config> configs = Arrays.stream(topics).map(topic -> {
        final String configFile = String
          .format("%s/%s.%s", databaseConfigFilesLocation, StringUtils.trim(topic), "yml");
        log.info("Loading config file {} for topic {}", configFile, topic);
        final Config config = config(configFile);
        config.setTopic(topic);
        return config;
      }).collect(Collectors.toList());

      topicToClassNameMapping = configs.stream()
        .collect(Collectors.toMap(Config::getTopic, Function.identity()));
      log.info("{} Topic configurations are loaded.", topicToClassNameMapping.size());
      return this;
    }

    public OrientDbSinkResourceProvider build() {
      return new OrientDbSinkResourceProvider(this.topicToClassNameMapping);
    }

    @SneakyThrows
    private Config config(final String configFile) {
      return MAPPER.readValue(new File(configFile), Config.class);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @Getter
  private static class Config {

    @Setter
    private String topic;
    private String connectionString;
    private String database;
    private String className;
    private String username;
    private String password;

  }
}
