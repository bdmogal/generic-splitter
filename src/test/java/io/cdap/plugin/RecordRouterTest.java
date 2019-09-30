/*
 * Copyright © 2019 Cask Data, Inc.
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
public abstract class RecordRouterTest {
  static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("supplier_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("part_id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("count", Schema.of(Schema.Type.INT)));
  static final String PORT_SPECIFICATION = "a_port:equals(10)";

  public abstract String getMode();

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
    RecordRouter.Config config = new RecordRouter.Config("basic", "supplier_id", portSpecification, "a=b",
                                                         "Send to default port", "Default Port", "Null Port");
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

  private void testDefaultedRecord(String defaultHandling, @Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "1")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config("basic", "supplier_id", PORT_SPECIFICATION, "a=b",
                                                         defaultHandling, outputPortName, null);
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
    Assert.assertEquals(testRecord, record);
  }
}
