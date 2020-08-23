package com.sanjuthomas.orientdb.resolver;

import java.util.Map;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * @author Sanju Thomas
 */
public class SinkConnectorConfigResolver implements ParameterResolver {

  @Override
  public boolean supportsParameter(ParameterContext parameterContext,
    ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == Map.class;
  }

  @Override
  public Object resolveParameter(ParameterContext parameterContext,
    ExtensionContext extensionContext) throws ParameterResolutionException {
    return Map.of("topics", "open_weather_data,quote_request",
      "databaseConfigFileLocation", "src/test/resource");
  }
}
