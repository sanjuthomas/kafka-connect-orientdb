package com.sanjuthomas.orientdb.resolver;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;

/**
 * @author Sanju Thomas
 */
public class SinkRecordsResolver extends MapEventResolver {

  @Override
  public boolean supportsParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == List.class;
  }

  @Override
  public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    final Map<String, String> symbol = this.createData();
    return List.of(new SinkRecord("open_weather_data", 0, null, "MMM", null, symbol, -1,
        System.currentTimeMillis(), TimestampType.CREATE_TIME),
      new SinkRecord("quote_request", 0, null, "AAPL", null, symbol, -1,
        System.currentTimeMillis(), TimestampType.CREATE_TIME));
  }
}