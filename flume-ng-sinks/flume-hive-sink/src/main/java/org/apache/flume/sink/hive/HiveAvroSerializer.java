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

package org.apache.flume.sink.hive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hive.hcatalog.streaming.HiveAvroWriter;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.RecordWriter;
import org.apache.hive.hcatalog.streaming.StreamingException;
import org.apache.hive.hcatalog.streaming.TransactionBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveAvroSerializer implements HiveEventSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAvroSerializer.class);

  public static final String ALIAS = "AVRO";

  Schema schema = null;
  DatumReader<GenericRecord> datumReader = null;
  DatumWriter<GenericRecord> datumWriter = null;
  ByteArrayOutputStream buffer = new ByteArrayOutputStream();
  GenericRecord record = null;
  BinaryEncoder encoder = null;

  @Override
  public void write(TransactionBatch txnBatch, Event e)
      throws StreamingException, IOException, InterruptedException {
    write(txnBatch, e.getBody());
  }

  @Override
  public void write(TransactionBatch txnBatch, Collection<byte[]> events)
      throws StreamingException, IOException, InterruptedException {
    for (byte[] body : events) {
      write(txnBatch, body);
    }
  }

  void write(TransactionBatch txnBatch, byte[] body)
      throws StreamingException, IOException, InterruptedException {
    InputStream avroInputStream = new ByteArrayInputStream(body);
    DataFileStream<GenericRecord> avroDataReader = new DataFileStream<GenericRecord>(
        avroInputStream, datumReader);
    
    List<byte[]> data = new ArrayList<>();
    
    while (avroDataReader.hasNext()) {
      record = avroDataReader.next(record);
      buffer.reset();

      encoder = EncoderFactory.get().binaryEncoder(buffer, encoder);
      datumWriter.write(record, encoder);
      encoder.flush();
      
      data.add(buffer.toByteArray());
    }

    txnBatch.write(data);
    avroDataReader.close();
  }

  @Override
  public RecordWriter createRecordWriter(HiveEndPoint endPoint)
      throws StreamingException, IOException, ClassNotFoundException {
    HiveAvroWriter writer = new HiveAvroWriter(endPoint);

    schema = writer.getSchema();
    LOG.info("Hive table schema: \n{}", schema);
    
    datumReader = new GenericDatumReader<GenericRecord>(schema);
    datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    
    return writer;
  }

  @Override
  public void configure(Context context) {
    return;
  }

}
