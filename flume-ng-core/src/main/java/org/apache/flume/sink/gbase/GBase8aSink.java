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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * A Flume Sink that can publish messages to GBase 8a MPP cluster.
 */
public class GBase8aSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(GBase8aSink.class);

  private PassiveHttpSink httpSink;

  private CounterGroup counterGroup;
  private int batchSize;

  public GBase8aSink() {
    counterGroup = new CounterGroup();
    httpSink = new PassiveHttpSink();
  }

  @Override
  public void configure(Context context) {
    httpSink.configure(context);

    batchSize = context.getInteger(GBase8aSinkConstants.BATCH_SIZE,
        GBase8aSinkConstants.DFLT_BATCH_SIZE);
    logger.debug(this.getName() + " " + "batch size set to " + String.valueOf(batchSize));
    Preconditions.checkArgument(batchSize > 0, "Batch size must be > 0");
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    /*
     * TODO 等待合适时间通过 jdbc 通知 8a 来读取数据
     */

    return status;
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);

    counterGroup.setName(this.getName());
    super.start();
    httpSink.start();

    logger.info("GBase 8a sink {} started.", getName());
  }

  @Override
  public void stop() {
    logger.info("GBase 8a sink {} stopping...", getName());

    httpSink.stop();
    super.stop();

    logger.info("GBase 8a sink {} stopped. Event metrics: {}", getName(), counterGroup);
  }

  @Override
  public String toString() {
    return "GBase8a " + getName() + " { batchSize: " + batchSize + " }";
  }

  @Override
  public synchronized void setChannel(Channel channel) {
    httpSink.setChannel(channel);
    super.setChannel(channel);
  }

}
