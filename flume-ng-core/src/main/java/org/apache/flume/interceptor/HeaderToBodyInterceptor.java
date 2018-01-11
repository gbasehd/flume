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

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor that inserts into event body from event header.
 * <pre>
 * Example:
 * EventHeader:{topic=>hive,partition=>0}
 * EventBody:{"uuid":"35F0AAE45398F5BCE97B95A049DDA7AF"}</p> Configuration:
 * agent.sources.k.interceptors = htb
 * agent.sources.k.interceptors.htb.type = 
 * org.apache.flume.interceptor.HeaderToBodyInterceptor$Builder
 * agent.sources.k.interceptors.htb.bodyType = json
 * agent.sources.k.interceptors.htb.headerToBody = topic:string:topic,partition:int:part
 * results in an event with the the following
 *
 * body: {"uuid":"35F0AAE45398F5BCE97B95A049DDA7AF",
 * "topic":"hive","partition":0} 
 *
 * </pre>
 */
public class HeaderToBodyInterceptor implements Interceptor {

  private static final Logger log = LoggerFactory.getLogger(HeaderToBodyInterceptor.class);
  private static Gson gson = new Gson();

  private String bodyType = null;
  private String bodyCharset = null;
  private List<String[]> headerToBodyList = null;

  public HeaderToBodyInterceptor(String bodyType, List<String[]> headerToBodyList,
      String bodyCharset) {
    this.bodyType = bodyType;
    this.bodyCharset = bodyCharset;
    this.headerToBodyList = headerToBodyList;
  }

  @Override
  public void initialize() {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (final Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public Event intercept(Event event) {

    if (this.bodyType != null && !this.bodyType.isEmpty()) {
      
      if (log.isDebugEnabled()) {
        log.debug("bodyType : " + this.bodyType);
        log.debug("headerToBody : " + Arrays.deepToString(this.headerToBodyList.toArray()));
        log.debug("old event : " + new String(event.getBody()));
      }
      
      
      byte[] newBody = null;
      if (this.bodyType.equalsIgnoreCase("json")) {
        newBody = headerToBodyJson(event.getHeaders(), event.getBody());
      } else if (this.bodyType.equalsIgnoreCase("csv")) {
        newBody = headerToBodyCsv(event.getHeaders(), event.getBody());
      } else {
        newBody = event.getBody();
      }
      event.setBody(newBody);
      
      if (log.isDebugEnabled()) {
        log.debug("new event : " + new String(event.getBody()));
      }
    }

    return event;
  }

  private byte[] headerToBodyCsv(Map<String, String> headers, byte[] body) {
    
    String[] array = new String(body).split(",");
    ArrayList<String> arrayList = new ArrayList<String>(Arrays.asList(array));
    
    for (String[] item : this.headerToBodyList) {
      String itemHeader = item[0];
      //String itemType = item[1];
      String itemBody = item[2];
      String value;
      if (!headers.containsKey(itemHeader)) {
        log.warn(itemHeader + " isn't in EventHeader");
        value = "";
      } else {
        value = headers.get(itemHeader);
        log.debug("value of header {}: {}", itemHeader, value);
      }
      
      int itemPos = Integer.parseInt(itemBody);
      arrayList.add(itemPos, value);
    }
    StringBuilder payload = new StringBuilder();
    for (String str : arrayList) {
      payload.append(str).append(",");
    }
    payload.deleteCharAt(payload.length() - 1);
    
    byte[] newBody = null;
    try {
      newBody = payload.toString().getBytes(this.bodyCharset);
    } catch (UnsupportedEncodingException e) {
      log.error("csv to bytes error : {}", e);
    }
    return newBody;
  }

  @SuppressWarnings("unchecked")
  private byte[] headerToBodyJson(Map<String, String> headers, byte[] body) {

    Map<String, Object> payloadMap;
    try {
      payloadMap = gson.fromJson(new String(body, this.bodyCharset), Map.class);
    } catch (JsonSyntaxException | UnsupportedEncodingException e) {
      log.error("json from body error : {}", e);
      return null;
    }
    if (log.isDebugEnabled()) {
      log.debug("payloadMap: {}",payloadMap.toString());
      log.debug("header: {}",headers.toString());
    }
    
    for (String[] item : this.headerToBodyList) {
      String itemHeader = item[0];
      String itemType = item[1];
      String itemBody = item[2];
      
      log.debug("itemHeader: {} itemType: {} itemBody: {}",
          new String[] {itemHeader,itemType,itemBody});
      
      Object objValue = null;
      try {
        if (!headers.containsKey(itemHeader)) {
          log.warn(itemHeader + " isn't in EventHeader");
          continue;
        }
        String value = headers.get(itemHeader);
        log.debug("value of header {}: {}", new String[] {itemHeader, value});
        
        if (itemType.isEmpty() || itemType.equalsIgnoreCase("string")) {
          objValue = value;
        } else if (itemType.equalsIgnoreCase("int")) {
          objValue = Integer.parseInt(value);
        } else if (itemType.equalsIgnoreCase("long")) {
          objValue = Long.parseLong(value);
        } else if (itemType.equalsIgnoreCase("float")) {
          objValue = Float.parseFloat(value);
        } else if (itemType.equalsIgnoreCase("double")) {
          objValue = Double.parseDouble(value);
        } else {
          objValue = value;
        }
        payloadMap.put(itemBody, objValue);
      } catch (Exception e) {
        log.error("headerToBodyJson, {}", e);
      }
    }
    byte[] newBody = null;
    try {
      newBody = gson.toJson(payloadMap).getBytes(this.bodyCharset);
    } catch (UnsupportedEncodingException e) {
      log.error("json to bytes error : {}", e);
    }
    return newBody;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /**
   * Builder which builds new instances of the {@link RemoveHeaderInterceptor}.
   */
  public static class Builder implements Interceptor.Builder {
    private String bodyType = null;
    private String bodyCharset = null;
    private List<String[]> headerToBodyList = new ArrayList<>();
    static final String HEADER_TO_BODY = "headerToBody";
    static final String BODY_TYPE = "bodyType";
    static final String BODY_CHARSET = "bodyCharset";
    
    @Override
    public Interceptor build() {
      log.debug("Creating HeaderToBodyInterceptor with: bodyTpye : {} bodyCharset : {}", bodyType,
          bodyCharset);
      log.debug("headerToBodyList : {}", Arrays.deepToString(headerToBodyList.toArray()));
      return new HeaderToBodyInterceptor(bodyType, headerToBodyList,bodyCharset);
    }

    @Override
    public void configure(final Context context) {
      bodyType = context.getString(BODY_TYPE,"");
      bodyCharset = context.getString(BODY_CHARSET, "utf-8");
      log.debug("bodyType : {}",bodyType);
      if (bodyType != null && !bodyType.isEmpty()) {
        Preconditions.checkArgument(bodyType.equalsIgnoreCase("json")
            || bodyType.equalsIgnoreCase("csv"), "body type isn't json or csv");
      
        String[] headerToBodys = context.getString(HEADER_TO_BODY,"").split(",");      
        for (String item : headerToBodys) {
          String[] headerToBody = item.split(":");
          Preconditions.checkArgument(headerToBody.length == 3,
              "headerToBody must be three fields, like headerTopic:string:bodyTopic");
          headerToBodyList.add(headerToBody);
        }
      }
    }
  }
}
