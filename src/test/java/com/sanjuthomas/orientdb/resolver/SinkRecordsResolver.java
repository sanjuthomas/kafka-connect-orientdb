/*
 *  Copyright (c) 2020 Sanju Thomas
 *
 *  Licensed under the MIT License (the "License");
 *  You may not use this file except in compliance with the License.
 *
 *  You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  either express or implied.
 *
 *  See the License for the specific language governing permissions
 *  and limitations under the License.
 */

package com.sanjuthomas.orientdb.resolver;

import com.sanjuthomas.orientdb.bean.Account;
import com.sanjuthomas.orientdb.bean.Client;
import com.sanjuthomas.orientdb.bean.QuoteRequest;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

/**
 * @author Sanju Thomas
 */
public class SinkRecordsResolver implements ParameterResolver {

  @Override
  public boolean supportsParameter(final ParameterContext parameterContext,
    final ExtensionContext extensionContext) throws ParameterResolutionException {
    return parameterContext.getParameter().getType() == List.class;
  }

  @Override
  public Object resolveParameter(final ParameterContext parameterContext,
    final ExtensionContext extensionContext) throws ParameterResolutionException {
    final QuoteRequest quoteRequest1 = new QuoteRequest(UUID.randomUUID().toString(),
      "AAPL", 10, new Client("C-100", new Account("A-001")), ZonedDateTime.now());
    final QuoteRequest quoteRequest2 = new QuoteRequest(UUID.randomUUID().toString(),
      "MMM", 100, new Client("C-101", new Account("A-002")), ZonedDateTime.now());

    return List.of(new SinkRecord("open_weather_data", 0, null, "AAPL", null, quoteRequest1, 1,
        System.currentTimeMillis(), TimestampType.CREATE_TIME),
      new SinkRecord("quote_request", 0, null, "MMM", null, quoteRequest2, 2,
        System.currentTimeMillis(), TimestampType.CREATE_TIME));
  }
}