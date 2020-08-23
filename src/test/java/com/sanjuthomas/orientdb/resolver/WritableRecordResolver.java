package com.sanjuthomas.orientdb.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.orientdb.bean.Account;
import com.sanjuthomas.orientdb.bean.Client;
import com.sanjuthomas.orientdb.bean.QuoteRequest;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

/**
 * @author Sanju Thomas
 */
public class WritableRecordResolver extends MapEventResolver {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public boolean supportsParameter(final ParameterContext parameterContext,
    final ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == WritableRecord.class;
  }

  @Override
  @SneakyThrows
  public Object resolveParameter(final ParameterContext parameterContext,
    final ExtensionContext extensionContext) throws ParameterResolutionException {

    final QuoteRequest quoteRequest = new QuoteRequest(UUID.randomUUID().toString(),
      "AAPL", 10, new Client("C-100", new Account("A-001")), ZonedDateTime.now());

    return WritableRecord.builder()
      .className("QuoteRequest")
      .database("quote_request")
      .topic("quote_request")
      .jsonDocumentString(MAPPER.writeValueAsString(quoteRequest)).build();
  }
}