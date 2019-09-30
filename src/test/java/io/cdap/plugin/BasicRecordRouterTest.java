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

  @Test
  public void testEqualsFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:equals(supplierA),portB:equals(supplierB)");
  }

  @Test
  public void testNotEqualsFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_equals(supplierA),portB:not_equals(supplierB)");
  }

  @Test
  public void testContainsFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:contains(plier),portB:contains(flier)");
  }

  @Test
  public void testNotContainsFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_contains(plier),portB:not_contains(flier)");
  }

  @Test
  public void testInFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:in(supplierA|supplierB),portB:in(suppA|suppB)");
  }

  @Test
  public void testNotInFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_in(supplierA|supplierB),portB:not_in(s|suppB)");
  }

  @Test
  public void testMatchesFunction() throws Exception {
    testBasicFunction("portA", "portB", "portA:matches(.*plierA$),portB:matches(.*flierA$)");
  }

  @Test
  public void testNotMatchesFunction() throws Exception {
    testBasicFunction("portB", "portA", "portA:not_matches(.*plierA$),portB:not_matches(.*xyz$)");
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

    Assert.assertEquals(testRecord, record);
  }

  private void testBasicFunction(String portToRouteTo, String portToNotRouteTo,
                                 String portSpecification) throws Exception {
    StructuredRecord testRecord = StructuredRecord.builder(INPUT)
      .set("supplier_id", "supplierA")
      .set("part_id", "2")
      .set("count", "3")
      .build();

    RecordRouter.Config config = new RecordRouter.Config(
      getMode(), "supplier_id", portSpecification, "port:1==1", null, null, null
    );
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    recordRouter.transform(testRecord, emitter);

    List<Object> objects = emitter.getEmitted().get(portToRouteTo);
    StructuredRecord record = (StructuredRecord) objects.get(0);
    Assert.assertEquals(testRecord, record);
    objects = emitter.getEmitted().get(portToNotRouteTo);
    Assert.assertNull(objects);
  }
}
