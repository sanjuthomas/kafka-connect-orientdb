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

package com.sanjuthomas.orientdb.writer;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import java.time.Duration;

/**
 * @author Sanju Thomas
 */
public class WriteRetryRegistry {

  public Retry retry() {
    final RetryConfig config = RetryConfig.custom()
      .maxAttempts(2)
      .waitDuration(Duration.ofMillis(1000))
      .failAfterMaxAttempts(true)
      .build();
    final RetryRegistry registry = RetryRegistry.of(config);
    return registry.retry("OrientDBWriter");
  }
}
