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

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.HeaderJoinInterceptor.Constants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestHeaderJoinInterceptor {
  private Interceptor.Builder builder;
  
  @Before
  public void setup() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    builder = new org.apache.flume.interceptor.HeaderJoinInterceptor.Builder();
  }
  
  @Test
  public void testDefaultKeyValue() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    builder.configure(new Context());
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    Assert.assertNull(event.getHeaders().get(Constants.KEY));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY);

    Assert.assertNotNull(val);
    Assert.assertEquals(Constants.JOIN_KEY_DEFAULT_VALUES_DEFAULT, val);
  }

  @Test
  public void testCustomKeyValue() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Context ctx = new Context();
    ctx.put(Constants.JOIN_KEYS, "topic partition");
    ctx.put(Constants.JOIN_DELIMITER, ":");
    ctx.put(Constants.KEY, "myKey");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Map<String, String> headers = new HashMap<String, String>();
    headers.put("topic", "myTopic");
    headers.put("partition", "1");
    
    Event event = EventBuilder.withBody("test", Charsets.UTF_8, headers);
    Assert.assertNull(event.getHeaders().get("myKey"));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get("myKey");

    Assert.assertNotNull(val);
    Assert.assertEquals("myTopic:1", val);
  }

  @Test
  public void testReplace() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Context ctx = new Context();
    ctx.put(Constants.JOIN_KEYS, "topic partition");
    ctx.put(Constants.PRESERVE, "false");
    ctx.put(Constants.JOIN_KEY_DEFAULT_VALUES, "replacement value");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    event.getHeaders().put(Constants.KEY_DEFAULT, "incumbent+value");

    Assert.assertNotNull(event.getHeaders().get(Constants.KEY_DEFAULT));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY_DEFAULT);

    Assert.assertNotNull(val);
    Assert.assertEquals("replacement-value", val);
  }

  @Test
  public void testPreserve() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Context ctx = new Context();
    ctx.put(Constants.PRESERVE, "true");
    ctx.put(Constants.JOIN_KEY_DEFAULT_VALUES, "replacement+value");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    event.getHeaders().put(Constants.KEY_DEFAULT, "incumbent+value");

    Assert.assertNotNull(event.getHeaders().get(Constants.KEY_DEFAULT));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY_DEFAULT);

    Assert.assertNotNull(val);
    Assert.assertEquals("incumbent+value", val);
  }
}