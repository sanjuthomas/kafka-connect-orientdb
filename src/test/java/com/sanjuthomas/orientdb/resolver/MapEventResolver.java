package com.sanjuthomas.orientdb.resolver;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * @author Sanju Thomas
 */
public class MapEventResolver implements ParameterResolver {

  @Override
  public boolean supportsParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == Map.class;
  }

  @Override
  public Object resolveParameter(final ParameterContext parameterContext,
      final ExtensionContext extensionContext) throws ParameterResolutionException {
    return this.createData();
  }

  protected Map<String, String> createData() {
    final Map<String, String> symbol = new HashMap<>();
    symbol.put("symbol", "MMM");
    symbol.put("name", "3MCompany");
    symbol.put("sector", "Industrials");
    symbol.put("exchange", "NYQ");
    return symbol;
  }
}