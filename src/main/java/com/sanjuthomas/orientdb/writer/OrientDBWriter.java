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

import com.sanjuthomas.orientdb.bean.WritableRecord;
import com.sanjuthomas.orientdb.writer.OrientDBConnectionBuilder.OrientDBConnection;
import io.github.resilience4j.retry.Retry;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Sanju Thomas
 */

@Slf4j
public class OrientDBWriter {

  private final OrientDBConnectionBuilder connectionBuilder;
  private final OrientDBConnection orientDBConnection;
  private final WriteRetryRegistry registry = new WriteRetryRegistry();

  public OrientDBWriter(final OrientDBConnectionBuilder connectionBuilder) {
    this.connectionBuilder = connectionBuilder;
    orientDBConnection = connectionBuilder.build();
  }

  public void write(final List<WritableRecord> writableRecords) {
    final WriteOperations writeOperations = new WriteOperations(orientDBConnection);
    try {
      Retry.decorateFunction(registry.retry(), writeOperations).apply(writableRecords);
    } catch(Exception e) {
      orientDBConnection.getDocument().rollback();
      throw e;
    }
  }

  public void close() {
    if (null != orientDBConnection.getDocument()) {
      orientDBConnection.getDocument().close();
    }
    if (null != orientDBConnection.getOrientDB()) {
      orientDBConnection.getOrientDB().close();
    }
  }
}
