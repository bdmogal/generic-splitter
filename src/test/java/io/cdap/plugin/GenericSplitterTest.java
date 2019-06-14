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
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockMultiOutputEmitter;
import io.cdap.plugin.GenericSplitter;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * This is an example of how you can build unit tests for your transform.
 */
public class GenericSplitterTest {
  private static final Schema INPUT =
    Schema.recordOf("input",
                    Schema.Field.of("splitField", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("b", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("c", Schema.of(Schema.Type.STRING)));
  @Test
  public void testNullSplitFieldDefaultPort() throws Exception {
    testNullSplitField(null);
  }

  @Test
  public void testNullSplitFieldCustomPort() throws Exception {
    testNullSplitField("nullPort");
  }

  private void testNullSplitField(@Nullable String nullPortName) throws Exception {
    GenericSplitter.Config config = new GenericSplitter.Config("splitField", nullPortName, INPUT.toString());
    SplitterTransform<StructuredRecord, StructuredRecord> transform = new GenericSplitter(config);
    transform.initialize(null);

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    transform.transform(StructuredRecord.builder(INPUT)
                          .set("splitField", null)
                          .set("b", "2")
                          .set("c", "3").build(), emitter);
    nullPortName = nullPortName == null ? "Null" : nullPortName;
    StructuredRecord record = (StructuredRecord) emitter.getEmitted().get(nullPortName).get(0);
    Assert.assertNull(record.get("splitField"));
    Assert.assertEquals("2", record.get("b"));
    Assert.assertEquals("3", record.get("c"));
  }
}
