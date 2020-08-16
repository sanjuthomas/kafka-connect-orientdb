package com.sanjuthomas.orientdb.sink.bean;

import lombok.Builder;
import lombok.Getter;

/**
 * @author Sanju Thomas
 */

@Getter
@Builder
public class WriteResult {

  private final String className;
  private final Integer recordsWritten;
  private final int documentCount;

}
