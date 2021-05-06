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

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author Sanju Thomas
 */
@RequiredArgsConstructor
public class OrientDBConnectionBuilder {

  private final WriterConfig configuration;

  public OrientDBConnection build() {
    final OrientDB db = new OrientDB(configuration.getConnectionString(),
      configuration.getUsername(), configuration.getPassword(),
      OrientDBConfig.defaultConfig());
    if (configuration.getType() == ODatabaseType.MEMORY) {
      db.createIfNotExists(configuration.getDatabase(), configuration.getType(),
        OrientDBConfig.defaultConfig());
    }
    final ODatabaseDocument document = db
      .open(configuration.getDatabase(), configuration.getUsername(),
        configuration.getPassword());
    document.createClassIfNotExist(configuration.getClassName());
    return new OrientDBConnection(configuration.getDatabase(), document, db);
  }

  @RequiredArgsConstructor
  @Getter
  public static class OrientDBConnection {
    private final String databaseName;
    private final ODatabaseDocument document;
    private final OrientDB orientDB;
  }
}
