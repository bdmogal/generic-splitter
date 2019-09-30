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

  JexlSpecificationEvaluator(String portSpecification) {
    JexlEngine engine = new JexlBuilder()
      .namespaces(NAMESPACES)
      .silent(false)
      .cache(1024)
      .strict(true)
      .create();
    this.context = new MapContext();
    this.portSpecifications = parse(portSpecification, engine);
  }

  @Override
  public void validate() {

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
      throw new IllegalArgumentException("fields can't be null");
    }
    for (Schema.Field field : fields) {
      String variableName = field.getName();
      context.set(variableName, record.get(variableName));
    }
    for (JexlPortSpecification portSpecification : portSpecifications) {
      Object result = portSpecification.getJexlScript().execute(context);
      if (!(result instanceof Boolean)) {
        throw new IllegalArgumentException(
          String.format("Illegal JEXL expression %s. Please make sure that the expression returns a boolean value.",
                        portSpecification.getJexlScript().getSourceText())
        );
      }
      Boolean selectPort = (Boolean) result;
      if (selectPort) {
        return portSpecification.getName();
      }
    }
    return null;
  }

  private static List<JexlPortSpecification> parse(String portSpecification, JexlEngine engine) {
    List<JexlPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    for (String singlePortSpecification : Splitter.on(',').trimResults().split(portSpecification)) {
      int colonIdx = singlePortSpecification.indexOf(':');
      if (colonIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find ':' separating port name from its selection operation in '%s'.", singlePortSpecification));
      }
      String portName;
      try {
        portName = URLDecoder.decode(singlePortSpecification.substring(0, colonIdx).trim(), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // This should never happen
        throw new IllegalStateException("Unsupported encoding when trying to decode port name.", e);
      }
      if (!portNames.add(portName)) {
        throw new IllegalArgumentException(String.format(
          "Cannot create multiple ports with the same name '%s'.", portName));
      }

      String expression;
      try {
        expression = URLDecoder.decode(singlePortSpecification.substring(colonIdx + 1).trim(), Charsets.UTF_8.name());
      } catch (UnsupportedEncodingException e) {
        // This should never happen
        throw new IllegalStateException("Unsupported encoding when trying to decode port name.", e);
      }
      portSpecifications.add(new JexlPortSpecification(portName, engine.createScript(expression)));
    }

    if (portSpecifications.isEmpty()) {
      throw new IllegalArgumentException("The 'portSpecifications' property must be set.");
    }
    return portSpecifications;
  }
}
