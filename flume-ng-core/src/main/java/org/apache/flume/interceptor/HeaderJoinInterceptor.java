/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor class that join two or more headers to a new header of all events.
 *
 * Properties:<p>
 *   joinKeys: Headers to join together.
 *        (default is "key")<p>
 *   
 *   joinKeyDefaultValues: default value to keys.
 *        (default is "NA")<p>
 *   
 *   joinDelimiter: join delimiter.
 *        (default is "-")<p>
 *        
 *   key: Key to use in join header insertion.
 *        (default is "key")<p>
 *
 *   preserveExisting: Whether to preserve an existing value for 'key'
 *                     (default is true)<p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = join<p>
 *   agent.sources.r1.interceptors.i1.joinKeys = topic partition<p>
 *   agent.sources.r1.interceptors.i1.joinKeyDefaultValues = NA<p>
 *   agent.sources.r1.interceptors.i1.joinDelimiter = -<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = false<p>
 *   agent.sources.r1.interceptors.i1.key = datacenter<p>
 * </code>
 *
 */
public class HeaderJoinInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory.getLogger(HeaderJoinInterceptor.class);

  private final String[] joinKeys;
  private final String[] joinKeyDefaultValues;
  private final String joinDelimiter;
  private final boolean preserveExisting;
  private final String key;

  /**
   * Only {@link HostInterceptor.Builder} can build me
   */
  private HeaderJoinInterceptor(String joinKeys, String joinKeyDefaultValues,
          String joinDelimiter, boolean preserveExisting,
          String key) {
    this.joinKeys = joinKeys.split(" ");
    this.joinKeyDefaultValues = joinKeyDefaultValues.split(" ");
    this.joinDelimiter = joinDelimiter;
    this.preserveExisting = preserveExisting;
    this.key = key;
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();

    if (preserveExisting && headers.containsKey(key)) {
      return event;
    }
    
    String[] values = new String[this.joinKeys.length];
    for (int i = 0; i < this.joinKeys.length; ++i) {
      values[i] = headers.getOrDefault(this.joinKeys[i],
        this.joinKeyDefaultValues[i % this.joinKeyDefaultValues.length]);
    }
    headers.put(key, String.join(this.joinDelimiter, values));
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instance of the HeaderJoinInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String joinKeys;
    private String joinKeyDefaultValues;
    private String joinDelimiter;
    private boolean preserveExisting;
    private String key;

    @Override
    public void configure(Context context) {
      joinKeys = context.getString(Constants.JOIN_KEYS, Constants.JOIN_KEYS_DEFAULT);
      joinKeyDefaultValues = context.getString(Constants.JOIN_KEY_DEFAULT_VALUES, 
              Constants.JOIN_KEY_DEFAULT_VALUES_DEFAULT);
      joinDelimiter = context.getString(Constants.JOIN_DELIMITER, Constants.JOIN_DELIMITER_DEFAULT);
      preserveExisting = context.getBoolean(Constants.PRESERVE, Constants.PRESERVE_DEFAULT);
      key = context.getString(Constants.KEY, Constants.KEY_DEFAULT);
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating HeaderJoinInterceptor: joinKeys=%s,joinKeyDefaultValues=%s,"
          + "joinDelimiter=%s,preserveExisting=%s,key=%s",
          joinKeys, joinKeyDefaultValues, joinDelimiter, preserveExisting, key));
      return new HeaderJoinInterceptor(joinKeys, joinKeyDefaultValues, joinDelimiter, 
              preserveExisting, key);
    }

  }

  public static class Constants {
    public static final String JOIN_KEYS = "joinKeys";
    public static final String JOIN_KEYS_DEFAULT = "key";
    
    public static final String JOIN_KEY_DEFAULT_VALUES = "joinKeyDefaultValues";
    public static final String JOIN_KEY_DEFAULT_VALUES_DEFAULT = "NA";
    
    public static final String JOIN_DELIMITER = "joinDelimiter";
    public static final String JOIN_DELIMITER_DEFAULT = "-";
    
    public static final String KEY = "key";
    public static final String KEY_DEFAULT = "key";

    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;
  }
}
