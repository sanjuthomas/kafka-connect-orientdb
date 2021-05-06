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

/**
 * @author Sanju Thomas
 */

import com.orientechnologies.orient.core.db.ODatabaseType;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class WriterConfig {

  private ODatabaseType type;
  private String connectionString;
  private String database;
  private String className;
  private String username;
  private String password;
}
