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

package org.apache.hive.hcatalog.streaming;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.rmi.server.UID;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class HiveAvroWriter extends AbstractRecordWriter {
  private AvroSerDe serde;

  Schema schema = null;
  DatumReader<GenericRecord> datumReader = null;
  GenericRecord record = null;
  BinaryDecoder decoder = null;
  UID recordReaderID = null;

  private final StandardStructObjectInspector recordObjInspector;
  private final ObjectInspector[] bucketObjInspectors;
  private final StructField[] bucketStructFields;

  public HiveAvroWriter(HiveEndPoint endPoint)
      throws ConnectionError, SerializationError, StreamingException {
    this(endPoint, null, null);
  }

  public HiveAvroWriter(HiveEndPoint endPoint, HiveConf conf) throws StreamingException {
    this(endPoint, conf, null);
  }

  public HiveAvroWriter(HiveEndPoint endPoint, StreamingConnection conn)
      throws ConnectionError, SerializationError, StreamingException {
    this(endPoint, null, conn);
  }

  /**
   * Hive Avro Writer is the avro writer to hive.
   * 
   * @param endPoint
   *          the end point to write to
   * @param conf
   *          a Hive conf object. Should be null if not using advanced Hive settings.
   * @param conn
   *          connection this Writer is to be used with
   * @throws ConnectionError
   *           connection error
   * @throws SerializationError
   *           serialization error
   * @throws StreamingException
   *           streaming exception
   */
  public HiveAvroWriter(HiveEndPoint endPoint, HiveConf conf, StreamingConnection conn)
      throws ConnectionError, SerializationError, StreamingException {
    super(endPoint, conf, conn);
    Properties tableProps = MetaStoreUtils.getTableMetadata(tbl);

    this.serde = createSerde(tbl, conf, tableProps);
    this.schema = serde.determineSchemaOrReturnErrorSchema(conf, tableProps);
    this.datumReader = new GenericDatumReader<GenericRecord>(schema);
    this.recordReaderID = new UID();

    // get ObjInspectors for entire record and bucketed cols
    try {
      recordObjInspector = (StandardStructObjectInspector) serde.getObjectInspector();
      bucketObjInspectors = getObjectInspectorsForBucketedCols(bucketIds, recordObjInspector);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to get ObjectInspector for bucket columns", e);
    }

    // get StructFields for bucketed cols
    bucketStructFields = new StructField[bucketIds.size()];
    List<? extends StructField> allFields = recordObjInspector.getAllStructFieldRefs();
    for (int i = 0; i < bucketIds.size(); i++) {
      bucketStructFields[i] = allFields.get(bucketIds.get(i));
    }
  }

  @Override
  public AbstractSerDe getSerde() {
    return serde;
  }

  protected StandardStructObjectInspector getRecordObjectInspector() {
    return recordObjInspector;
  }

  @Override
  protected StructField[] getBucketStructFields() {
    return bucketStructFields;
  }

  protected ObjectInspector[] getBucketObjectInspectors() {
    return bucketObjInspectors;
  }

  @Override
  public void write(long transactionId, byte[] record)
      throws StreamingIOFailure, SerializationError {
    try {
      Object encodedRow = encode(record);
      int bucket = getBucket(encodedRow);
      getRecordUpdater(bucket).insert(transactionId, encodedRow);
    } catch (IOException e) {
      throw new StreamingIOFailure("Error writing record in transaction(" + transactionId + ")", e);
    }

  }

  /**
   * Creates AvroSerDe.
   * 
   * @param tbl
   *          used to create serde
   * @param conf
   *          used to create serde
   * @return
   * 
   * @throws SerializationError
   *           if serde could not be initialized
   */
  private static AvroSerDe createSerde(Table tbl, HiveConf conf, Properties tableProps)
      throws SerializationError {
    try {
      AvroSerDe serde = new AvroSerDe();
      SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde " + AvroSerDe.class.getName(), e);
    }
  }

  @Override
  public Object encode(byte[] datum) throws SerializationError {
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(datum);
      decoder = DecoderFactory.get().binaryDecoder(in, decoder);

      record = datumReader.read(record, decoder);

      AvroGenericRecordWritable writable = new AvroGenericRecordWritable();
      writable.setRecord(record);
      writable.setFileSchema(schema);
      writable.setRecordReaderID(recordReaderID);

      return serde.deserialize(writable);
    } catch (SerDeException | IOException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

  public Schema getSchema() {
    return schema;
  }
}
