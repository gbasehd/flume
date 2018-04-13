/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.gbase;

public class GBase8aSinkConstants {

  public static final String GBASE8A_PREFIX = "gbase8a.";

  /* Properties */
  public static final String BATCH_SIZE = "batchSize";
  public static final String INTERVAL = "interval";
  public static final String CONNECTION_STRING = GBASE8A_PREFIX + "jdbcUrl";
  public static final String GBASE8A_DATABASE = GBASE8A_PREFIX + "database";
  public static final String GBASE8A_TABLE = GBASE8A_PREFIX + "table";

  public static final int DFLT_BATCH_SIZE = 10000; // events
  public static final int DFLT_INTERVAL = 10000; // ms
  public static final String DFLT_HANDLER = "org.apache.flume.sink.gbase.PassiveHttpSink$CHUNKHandler";
}
