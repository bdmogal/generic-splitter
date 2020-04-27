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
import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A {@link PortSpecificationEvaluator} that evaluates port specifications in the basic routing mode
 */
public class BasicPortSpecificationEvaluator implements PortSpecificationEvaluator {
  private static final Logger LOG = LoggerFactory.getLogger(BasicPortSpecification.class);

  private final String routingField;
  private final String nullPort;
  private final List<BasicPortSpecification> portSpecifications;

  BasicPortSpecificationEvaluator(String routingField, String nullPort, String portSpecification,
                                  FailureCollector collector) {
    this.routingField = routingField;
    this.nullPort = nullPort;
    this.portSpecifications = parse(portSpecification, collector);
  }

  @Override
  public List<String> getAllPorts() {
    List<String> ports = new ArrayList<>();
    for (BasicPortSpecification specification : portSpecifications) {
      ports.add(specification.getName());
    }
    return ports;
  }

  private static List<BasicPortSpecification> parse(String portSpecification, FailureCollector collector) {
    List<BasicPortSpecification> portSpecifications = new ArrayList<>();
    Set<String> portNames = new HashSet<>();
    if (Strings.isNullOrEmpty(portSpecification)) {
      collector.addFailure(
        "Could not find any port specifications.", "At least one port specification must be provided."
      ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      throw collector.getOrThrowException();
    }
    for (String singlePortSpec : Splitter.on(',').trimResults().split(portSpecification)) {
      int colonIdx = singlePortSpec.indexOf(':');
      if (colonIdx < 0) {
        collector.addFailure(
          String.format(
            "Could not find ':' separating port name from its routing specification in '%s'.", singlePortSpec
          ), "The configuration for each port should contain a port name and its routing specification separated by :"
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String portName = singlePortSpec.substring(0, colonIdx).trim();
      if (!portNames.add(portName)) {
        collector.addFailure(
          String.format("Cannot create multiple ports with the same name '%s'.", portName),
          "Please specify a unique port name for each specification"
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      String functionAndParameter = singlePortSpec.substring(colonIdx + 1).trim();
      int leftParanIdx = functionAndParameter.indexOf('(');
      if (leftParanIdx < 0) {
        collector.addFailure(
          String.format("Could not find '(' in function '%s'. ", functionAndParameter),
          "Please specify routing functions as as function(parameter)."
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String functionStr = functionAndParameter.substring(0, leftParanIdx).trim();
      RecordRouter.Config.FunctionType function;
      try {
        function = RecordRouter.Config.FunctionType.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        collector.addFailure(
          String.format("Invalid routing function '%s'.", functionStr),
          String.format(
            "A routing function must be  one of %s.", Joiner.on(',').join(RecordRouter.Config.FunctionType.values())
          )
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
        throw collector.getOrThrowException();
      }

      if (!functionAndParameter.endsWith(")")) {
        collector.addFailure(
          String.format("Could not find closing ')' in function '%s'.", functionAndParameter),
          "Functions must be specified as function(parameter)"
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
      String parameter = functionAndParameter.substring(leftParanIdx + 1, functionAndParameter.length() - 1).trim();
      if (parameter.isEmpty()) {
        collector.addFailure(
          String.format("Invalid function '%s'.", functionAndParameter),
          "A parameter must be provided as an argument."
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }

      LOG.debug("Adding port config: name = {}; function = {}; parameter = {}", portName, function, parameter);
      portSpecifications.add(new BasicPortSpecification(portName, function, parameter, collector));
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
