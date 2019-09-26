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
import io.cdap.cdap.etl.mock.transform.MockTransformContext;
import org.junit.Assert;
import org.junit.Test;

public class JexlRouterTest extends RecordRouterTest {
  @Override
  public String getMode() {
    return "jexl";
  }

  @Test
  public void testSimpleJexl() throws Exception {
    StructuredRecord inputRecord1 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "a")
      .set("part_id", "a1")
      .set("count", "3")
      .build();

    StructuredRecord inputRecord2 = StructuredRecord.builder(INPUT)
      .set("supplier_id", "b")
      .set("part_id", "b1")
      .set("count", "6")
      .build();

    String jexlPortSpec = "A:stringutils%3AstartsWith(part_id%2C'a'),B:stringutils%3AstartsWith(part_id%2C'b')";
    RecordRouter.Config config = new RecordRouter.Config(getMode(), null, null, jexlPortSpec, null, null, null);
    SplitterTransform<StructuredRecord, StructuredRecord> recordRouter = new RecordRouter(config);
    recordRouter.initialize(new MockTransformContext());

    MockMultiOutputEmitter<StructuredRecord> emitter = new MockMultiOutputEmitter<>();
    recordRouter.transform(inputRecord1, emitter);
    recordRouter.transform(inputRecord2, emitter);
    StructuredRecord outputRecord1 = (StructuredRecord) emitter.getEmitted().get("A").get(0);
    StructuredRecord outputRecord2 = (StructuredRecord) emitter.getEmitted().get("B").get(0);
    Assert.assertEquals(inputRecord1, outputRecord1);
    Assert.assertEquals(inputRecord2, outputRecord2);
  }
}
