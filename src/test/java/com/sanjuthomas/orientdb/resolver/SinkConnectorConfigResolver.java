/*
 * Copyright (c) 2021 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 */

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
      "database.config.files.location", "src/test/resource");
  }
}
