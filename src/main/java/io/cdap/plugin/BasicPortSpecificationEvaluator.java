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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BasicPortSpecificationEvaluator implements PortSpecificationEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(BasicPortSpecification.class);

  private final String routingField;
  private final String nullPort;
  private List<BasicPortSpecification> portSpecifications;

  BasicPortSpecificationEvaluator(String routingField, String nullPort, String portSpecification) {
    this.routingField = routingField;
    this.nullPort = nullPort;
    this.portSpecifications = parse(portSpecification);
  }

  @Override
  public void validate() {

  }

  @Override
  public List<String> getAllPorts() {
    List<String> ports = new ArrayList<>();
    for (BasicPortSpecification specification : portSpecifications) {
      ports.add(specification.getName());
    }
    return ports;
  }

  private static List<BasicPortSpecification> parse(String portSpecification) {
    List<BasicPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    for (String singlePortSpecification : Splitter.on(',').trimResults().split(portSpecification)) {
      int colonIdx = singlePortSpecification.indexOf(':');
      if (colonIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find ':' separating port name from its selection operation in '%s'.", singlePortSpecification));
      }
      String portName = singlePortSpecification.substring(0, colonIdx).trim();
      if (!portNames.add(portName)) {
        throw new IllegalArgumentException(String.format(
          "Cannot create multiple ports with the same name '%s'.", portName));
      }

      String functionAndParameter = singlePortSpecification.substring(colonIdx + 1).trim();
      int leftParanIdx = functionAndParameter.indexOf('(');
      if (leftParanIdx < 0) {
        throw new IllegalArgumentException(String.format(
          "Could not find '(' in function '%s'. Operations must be specified as function(parameter).",
          functionAndParameter));
      }
      String functionStr = functionAndParameter.substring(0, leftParanIdx).trim();
      RecordRouter.Config.FunctionType function;
      try {
        function = RecordRouter.Config.FunctionType.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. Must be one of %s.", functionStr, Joiner.on(',').join(RecordRouter.Config.FunctionType.values())));
      }

      if (!functionAndParameter.endsWith(")")) {
        throw new IllegalArgumentException(String.format(
          "Could not find closing ')' in function '%s'. Functions must be specified as function(parameter).",
          functionAndParameter));
      }
      String parameter = functionAndParameter.substring(leftParanIdx + 1, functionAndParameter.length() - 1).trim();
      if (parameter.isEmpty()) {
        throw new IllegalArgumentException(String.format(
          "Invalid function '%s'. A parameter must be given as an argument.", functionAndParameter));
      }

      LOG.debug("Adding port config: name = {}; function = {}; parameter = {}", portName, function, parameter);
      portSpecifications.add(new BasicPortSpecification(portName, function, parameter));
    }

    if (portSpecifications.isEmpty()) {
      throw new IllegalArgumentException("The 'portSpecifications' property must be set.");
    }
    return portSpecifications;
  }

  @Override
  public String getPort(StructuredRecord record) {
    Object value = record.get(routingField);
    // if the value is null, emit it based on the selected null handling option
    if (value == null) {
      LOG.trace("Found null value for {}. Emitting to the null port {}.", routingField, nullPort);
      return nullPort;
    }
    String textValue = String.valueOf(value);
    for (BasicPortSpecification portSpecification : portSpecifications) {
      String portName = portSpecification.getName();
      BasicRoutingFunction routingFunction = portSpecification.getRoutingFunction();
      if (routingFunction.evaluate(textValue, portSpecification.getParameter())) {
        return portName;
      }
    }

    return null;
  }
}
