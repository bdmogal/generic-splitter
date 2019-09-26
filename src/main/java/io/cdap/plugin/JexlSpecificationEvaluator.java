/*
 *  Copyright © 2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.lang3.StringUtils;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link PortSpecificationEvaluator} that evaluates port specifications in the jexl routing mode
 */
public final class JexlSpecificationEvaluator implements PortSpecificationEvaluator {
  private static final Map<String, Object> NAMESPACES = new HashMap<>();
  static {
    NAMESPACES.put("math", Math.class);
    NAMESPACES.put("strings", Strings.class);
    NAMESPACES.put("bytes", Bytes.class);
    NAMESPACES.put("arrays", Arrays.class);
    NAMESPACES.put("stringutils", StringUtils.class);
  }

  private final List<JexlPortSpecification> portSpecifications;
  private final JexlContext context;
  private final FailureCollector collector;

  JexlSpecificationEvaluator(String portSpecification, FailureCollector collector) {
    JexlEngine engine = new JexlBuilder()
      .namespaces(NAMESPACES)
      .silent(false)
      .cache(1024)
      .strict(true)
      .create();
    this.context = new MapContext();
    this.collector = collector;
    this.portSpecifications = parse(portSpecification, engine, collector);
  }

  @Override
  public List<String> getAllPorts() {
    List<String> ports = new ArrayList<>();
    for (JexlPortSpecification portSpecification : portSpecifications) {
      ports.add(portSpecification.getName());
    }
    return ports;
  }

  @Override
  public String getPort(StructuredRecord record) {
    List<Schema.Field> fields = record.getSchema().getFields();
    if (fields == null) {
      collector.addFailure("Input fields must be provided", null);
      throw collector.getOrThrowException();
    }
    for (Schema.Field field : fields) {
      String variableName = field.getName();
      context.set(variableName, record.get(variableName));
    }
    for (JexlPortSpecification portSpecification : portSpecifications) {
      Object result = portSpecification.getJexlScript().execute(context);
      if (!(result instanceof Boolean)) {
        collector.addFailure(
          String.format("Invalid Jexl expression %s.", portSpecification.getJexlScript().getSourceText()),
          "Please make sure that the expression returns a boolean value."
        ).withConfigProperty(RecordRouter.Config.JEXL_PORT_SPECIFICATION_PROPERTY_NAME);
        throw collector.getOrThrowException();
      }
      Boolean selectPort = (Boolean) result;
      if (selectPort) {
        return portSpecification.getName();
      }
    }
    return null;
  }

  private List<JexlPortSpecification> parse(String portSpecification, JexlEngine engine, FailureCollector collector) {
    List<JexlPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    for (String singlePortSpec : Splitter.on(',').trimResults().split(portSpecification)) {
      int colonIdx = singlePortSpec.indexOf(':');
      if (colonIdx < 0) {
        collector.addFailure(
          String.format(
            "Could not find ':' separating port name from its Jexl routing expression in '%s'.", singlePortSpec
          ), "The configuration for each port should contain a port name and its Jexl routing expression separated " +
            "by :"
        ).withConfigProperty(RecordRouter.Config.JEXL_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String portName;
      try {
        portName = URLDecoder.decode(singlePortSpec.substring(0, colonIdx).trim(), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // This should never happen
        throw new IllegalStateException("Unsupported encoding when trying to decode port name.", e);
      }
      if (!portNames.add(portName)) {
        collector.addFailure(
          String.format("Cannot create multiple ports with the same name '%s'.", portName),
          "Please specify a unique port name for each specification"
        ).withConfigProperty(RecordRouter.Config.JEXL_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      String expression;
      try {
        expression = URLDecoder.decode(singlePortSpec.substring(colonIdx + 1).trim(), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // This should never happen
        throw new IllegalStateException("Unsupported encoding when trying to decode port name.", e);
      }
      portSpecifications.add(new JexlPortSpecification(portName, engine.createScript(expression)));
    }

    if (portSpecifications.isEmpty()) {
      collector.addFailure("At least 1 port specification must be provided.", null)
        .withConfigProperty(RecordRouter.Config.JEXL_PORT_SPECIFICATION_PROPERTY_NAME);
    }
    return portSpecifications;
  }
}
