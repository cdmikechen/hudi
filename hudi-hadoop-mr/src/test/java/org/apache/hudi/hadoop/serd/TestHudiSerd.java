/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop.serd;

import org.apache.hadoop.io.LongWritable;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHudiSerd {

  @Test
  public void testWritableTimestampMicro() {
    HudiWritableTimestampMicroObjectInspector hwtoiMicro =
            new HudiWritableTimestampMicroObjectInspector();
    long timestampLong = System.currentTimeMillis();
    Timestamp testTimestamp = new Timestamp(timestampLong);

    LongWritable longWritable = new LongWritable();
    longWritable.set(timestampLong * 1000);

    Timestamp hiveTimestamp = hwtoiMicro.getPrimitiveJavaObject(longWritable);
    assertTrue(testTimestamp.equals(hiveTimestamp));
  }

  @Test
  public void testWritableTimestampMillis() {
    HudiWritableTimestampMillisObjectInspector hwtoiMillis =
            new HudiWritableTimestampMillisObjectInspector();
    long timestampLong = System.currentTimeMillis();
    Timestamp testTimestamp = new Timestamp(timestampLong);

    LongWritable longWritable = new LongWritable();
    longWritable.set(timestampLong);

    Timestamp hiveTimestamp = hwtoiMillis.getPrimitiveJavaObject(longWritable);
    assertTrue(testTimestamp.equals(hiveTimestamp));
  }


}
