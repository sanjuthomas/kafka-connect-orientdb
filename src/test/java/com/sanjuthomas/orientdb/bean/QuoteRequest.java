package com.sanjuthomas.orientdb.bean;

import java.time.ZonedDateTime;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Sanju Thomas
 */
@AllArgsConstructor
@Getter
public class QuoteRequest {
  private String id;
  private String symbol;
  private int quantity;
  private Client client;
  private ZonedDateTime timestamp;
}