/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop.serd;

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * hive avro package transform timestamp to long with timestamp-millis logical type,
 * so that we usually use timestamp-millis logical type cast to timestamp by default.
 */
public class HudiWritableTimestampMillisObjectInspector extends WritableTimestampObjectInspector {

  public HudiWritableTimestampMillisObjectInspector() {
    super();
  }

  @Override
  public TimestampWritable getPrimitiveWritableObject(Object o) {
    if (o instanceof LongWritable) {
      return (TimestampWritable) super.create(new Timestamp(((LongWritable) o).get()));
    }
    return super.getPrimitiveWritableObject(o);
  }

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    if (o instanceof LongWritable) {
      return new Timestamp(((LongWritable) o).get());
    }
    return super.getPrimitiveJavaObject(o);
  }

  @Override
  public Object copyObject(Object o) {
    if (o instanceof LongWritable) {
      return new TimestampWritable(new Timestamp(((LongWritable) o).get()));
    }
    return super.copyObject(o);
  }

  @Override
  public Object set(Object o, byte[] bytes, int offset) {
    if (o instanceof LongWritable) {
      o = super.create(new Timestamp(((LongWritable) o).get()));
    } else {
      ((TimestampWritable) o).set(bytes, offset);
    }
    return o;
  }

  @Override
  public Object set(Object o, Timestamp t) {
    if (t == null) {
      return null;
    }
    if (o instanceof LongWritable) {
      o = super.create(t);
    } else {
      ((TimestampWritable) o).set(t);
    }
    return o;
  }

  @Override
  public Object set(Object o, TimestampWritable t) {
    if (t == null) {
      return null;
    }
    if (o instanceof LongWritable) {
      o = super.create(new Timestamp(((LongWritable) o).get()));
    } else {
      ((TimestampWritable) o).set(t);
    }
    return o;
  }
}
