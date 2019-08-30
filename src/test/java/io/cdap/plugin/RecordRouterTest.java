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
                    Schema.Field.of("routingField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("c", Schema.of(Schema.Type.STRING)));
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

  private void testDefaultedRecordToDefaultPort(@Nullable String defaultPortName) throws Exception {
    testDefaultedRecord("Send to default port", defaultPortName);
  }

  private void testNullRecordToNullPort(@Nullable String nullPortName) throws Exception {
    testNullRecord("Send to null port", nullPortName);
  }

  private void testDefaultedRecord(String defaultHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("routingField", "1")
      .set("b", "2")
      .set("c", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config("routingField", PORT_SPECIFICATION,
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
    Assert.assertEquals("1", record.get("routingField"));
    Assert.assertEquals("2", record.get("b"));
    Assert.assertEquals("3", record.get("c"));
  }

  private void testNullRecord(String nullHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("routingField", null)
      .set("b", "2")
      .set("c", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config("routingField", PORT_SPECIFICATION,
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
    Assert.assertNull(record.get("routingField"));
    Assert.assertEquals("2", record.get("b"));
    Assert.assertEquals("3", record.get("c"));
  }
}
