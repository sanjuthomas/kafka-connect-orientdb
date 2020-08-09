package com.sanjuthomas.orientdb.sink.bean;

import lombok.Builder;
import lombok.Getter;

/**
 * @author Sanju Thomas
 */
@Builder
@Getter
public class WritableRecord {

  private String database;
  private String className;
  private String jsonDocumentString;

}
