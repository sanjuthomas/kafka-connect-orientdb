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
  private final boolean hasError;
  private final boolean retryable;
  private final int documentCount;

}
