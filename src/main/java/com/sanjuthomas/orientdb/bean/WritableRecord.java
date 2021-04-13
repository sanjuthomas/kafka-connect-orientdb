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

package com.sanjuthomas.orientdb.bean;

import lombok.Builder;
import lombok.Getter;

/**
 * @author Sanju Thomas
 */

@Builder
@Getter
public class WritableRecord {

  private String topic;
  private String database;
  private String className;
  private String keyField;
  private String keyValue;
  private String jsonDocumentString;

  public String upsertQuery() {
    return new StringBuilder().append("UPDATE ").append(className).append(" MERGE ")
      .append(jsonDocumentString).append(" UPSERT").append(" WHERE ")
      .append(keyField).append(" = ").append("'").append(keyValue).append("'").toString();
  }

  public String deleteQuery() {
    return new StringBuilder().append("DELETE from ").append(className)
      .append(" WHERE ").append(keyField).append(" = ").append("'")
      .append(keyValue).append("'").toString();
  }

}
