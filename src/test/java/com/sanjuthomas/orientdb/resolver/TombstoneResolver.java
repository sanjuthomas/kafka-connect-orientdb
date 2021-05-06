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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sanjuthomas.orientdb.bean.Account;
import com.sanjuthomas.orientdb.bean.Client;
import com.sanjuthomas.orientdb.bean.QuoteRequest;
import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.bean.WriteMode;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * @author Sanju Thomas
 */
public class TombstoneResolver implements ParameterResolver {

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
      .writeMode(WriteMode.INSERT)
      .keyField("id")
      .keyValue("value")
      .build();
  }
}