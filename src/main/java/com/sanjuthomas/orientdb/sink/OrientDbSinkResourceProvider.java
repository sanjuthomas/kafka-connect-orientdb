package com.sanjuthomas.orientdb.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.sanjuthomas.orientdb.sink.writer.OrientDBWriter;
import com.sanjuthomas.orientdb.sink.writer.OrientDBWriter.Configuration;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

/**
 * @author Sanju Thomas
 *
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

    final Config config = configMap.get(topic);
    final OrientDBWriter orientDBWriter = new OrientDBWriter(Configuration.builder()
      .connectionString(config.connectionString)
      .database(config.getDatabase())
      .username(config.getUsername())
      .password(config.getPassword())
      .build());

    writerMap.put(topic, orientDBWriter);
    return orientDBWriter;
  }

  public synchronized void removeWriter(final String topic) {
    writerMap.remove(topic);
  }

  public String className(final String topic) {
    return configMap.get(topic).getClassName();
  }

  public static class Builder {

    private Map<String, Config> topicToClassNameMapping;

    public Builder using(final String[] topics,
      final String configFileLocation) {

      final List<Config> configs = Arrays.stream(topics).map(topic -> {
        final String configFile = String
          .format("%s/%s.%s", configFileLocation, StringUtils.trim(topic), "yml");
        log.info("Loading config file {} for topic {}", configFile, topic);
        final Config config = config(configFile);
        config.setTopic(topic);
        return config;
      }).collect(Collectors.toList());

      final Map<String, Config> topicToClassNameMapping = configs.stream()
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

  @Getter
  private static class Config {

    private String topic;
    private String connectionString;
    private String database;
    private String className;
    private String username;
    private String password;
    private Integer batchSize;
    private Integer maxRetries;
    private Long retryBackOffMillis;

    void setTopic(final String topic) {
      this.topic = topic;
    }
  }
}
