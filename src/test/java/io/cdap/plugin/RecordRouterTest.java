/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class RecordRouterTest {
  private static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("supplier_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("part_id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("count", Schema.of(Schema.Type.INT)));
  private static final String PORT_SPECIFICATION = "a_port:equals(10)";

  @Test
  public void testDefaultedRecordToError() throws Exception {
    testDefaultedRecord("Send to error port", null);
  }

  @Test
  public void testDefaultedRecordToDefaultDefaultPort() throws Exception {
    testDefaultedRecordToDefaultPort(null);
  }

  @Test
  public void testDefaultedRecordToDefaultPort() throws Exception {
    testDefaultedRecordToDefaultPort("Send to default port");
  }

  @Test
  public void testDefaultedRecordSkipped() throws Exception {
    testDefaultedRecord("skip", null);
  }

  @Test
  public void testNullRecordToError() throws Exception {
    testNullRecord("Send to error port", null);
  }

  @Test
  public void testNullRecordToDefaultNullPort() throws Exception {
    testNullRecordToNullPort(null);
  }

  @Test
  public void testNullRecordToNullPort() throws Exception {
    testNullRecordToNullPort("Send to null port");
  }

  @Test
  public void testNullRecordSkipped() throws Exception {
    testDefaultedRecord("skip", null);
  }

  @Test
  public void testRouting() throws Exception {
    StructuredRecord testRecord1 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier1")
      .set("part_id", "1")
      .set("count", "10")
      .build();

    StructuredRecord testRecord2 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier2")
      .set("part_id", "2")
      .set("count", "20")
      .build();

    StructuredRecord testRecord3 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplier3")
      .set("part_id", "3")
      .set("count", "30")
      .build();

    String portSpecification = "Supplier 1:equals(supplier1),Supplier 2:equals(supplier2),Supplier 3:equals(supplier3)";
    RecordRouter.Config config = new RecordRouter.Config("supplier_id", portSpecification,
                                                         "Send to default port", "Default Port",
                                                         "Send to null port", "Null Port");
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();

    recordRouter.transform(testRecord1, emitter);
    recordRouter.transform(testRecord2, emitter);
    recordRouter.transform(testRecord3, emitter);

    Assert.assertEquals(testRecord1, emitter.getEmitted().get("Supplier 1").get(0));
    Assert.assertEquals(testRecord2, emitter.getEmitted().get("Supplier 2").get(0));
    Assert.assertEquals(testRecord3, emitter.getEmitted().get("Supplier 3").get(0));
  }

  private void testDefaultedRecordToDefaultPort(@Nullable String defaultPortName) throws Exception {
    testDefaultedRecord("Send to default port", defaultPortName);
  }

  private void testNullRecordToNullPort(@Nullable String nullPortName) throws Exception {
    testNullRecord("Send to null port", nullPortName);
  }

  private void testDefaultedRecord(String defaultHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "1")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config("supplier_id", PORT_SPECIFICATION,
                                                         defaultHandling, outputPortName, null, null);
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    recordRouter.transform(testRecord, emitter);
    StructuredRecord record;
    if ("Send to error port".equalsIgnoreCase(defaultHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? RecordRouter.Config.DEFAULT_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if ("skip".equalsIgnoreCase(defaultHandling)) {
        Assert.assertNull(objects);
        return;
      }
      record = (StructuredRecord) objects.get(0);
    }
    Assert.assertEquals("1", record.get("supplier_id"));
    Assert.assertEquals("2", record.get("part_id"));
    Assert.assertEquals("3", record.get("count"));
  }

  private void testNullRecord(String nullHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", null)
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config("supplier_id", PORT_SPECIFICATION,
                                                         null, null, nullHandling, outputPortName);
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    recordRouter.transform(testRecord, emitter);
    StructuredRecord record;
    if ("Send to error port".equalsIgnoreCase(nullHandling)) {
      InvalidEntry<StructuredRecord> invalidEntry = emitter.getErrors().get(0);
      record = invalidEntry.getInvalidRecord();
    } else {
      outputPortName = outputPortName == null ? RecordRouter.Config.DEFAULT_NULL_PORT_NAME : outputPortName;
      List<Object> objects = emitter.getEmitted().get(outputPortName);
      if ("skip".equalsIgnoreCase(nullHandling)) {
        Assert.assertNull(objects);
        return;
      }
      record = (StructuredRecord) objects.get(0);
    }
    Assert.assertNull(record.get("supplier_id"));
    Assert.assertEquals("2", record.get("part_id"));
    Assert.assertEquals("3", record.get("count"));
  }
}
