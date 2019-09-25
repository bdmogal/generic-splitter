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
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import javax.annotation.Nullable;

public class BasicRecordRouterTest extends RecordRouterTest {
  @Override
  public String getMode() {
    return "basic";
  }

  @Test
  public void testNullRecordToError() throws Exception {
    testNullRecord(null);
  }

  @Test
  public void testNullRecordToNullPort() throws Exception {
    testNullRecordToNullPort("null");
  }

  @Test
  public void testNullRecordToDefaultNullPort() throws Exception {
    testNullRecordToNullPort(null);
  }

  private void testNullRecordToNullPort(@Nullable String nullPortName) throws Exception {
    testNullRecord(nullPortName);
  }

  private void testNullRecord(@Nullable String outputPortName) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", null)
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config(getMode(), "supplier_id", PORT_SPECIFICATION, "port:1==1",
                                                         null, null, outputPortName);
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    recordRouter.transform(testRecord, emitter);
    outputPortName = outputPortName == null ? RecordRouter.Config.DEFAULT_NULL_PORT_NAME : outputPortName;
    List<Object> objects = emitter.getEmitted().get(outputPortName);
    StructuredRecord record = (StructuredRecord) objects.get(0);

    Assert.assertNull(record.get("supplier_id"));
    Assert.assertEquals("2", record.get("part_id"));
    Assert.assertEquals("3", record.get("count"));
  }
}