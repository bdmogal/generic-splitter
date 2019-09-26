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

import com.google.common.base.Joiner;
import io.cdap.cdap.etl.api.FailureCollector;

class BasicPortSpecification extends PortSpecification {
  private final BasicRoutingFunction routingFunction;
  private final String parameter;
  private final FailureCollector collector;

  BasicPortSpecification(String name, RecordRouter.Config.FunctionType functionType, String parameter,
                         FailureCollector collector) {
    super(name);
    this.routingFunction = fromFunctionType(functionType);
    this.parameter = parameter;
    this.collector = collector;
  }

  BasicRoutingFunction getRoutingFunction() {
    return routingFunction;
  }

  String getParameter() {
    return parameter;
  }

  private BasicRoutingFunction fromFunctionType(RecordRouter.Config.FunctionType functionType) {
    BasicRoutingFunction routingFunction;
    switch (functionType) {
      case EQUALS:
        routingFunction = new BasicRoutingFunctions.EqualsFunction();
        break;
      case NOT_EQUALS:
        routingFunction = new BasicRoutingFunctions.NotEqualsFunction();
        break;
      case CONTAINS:
        routingFunction = new BasicRoutingFunctions.ContainsFunction();
        break;
      case NOT_CONTAINS:
        routingFunction = new BasicRoutingFunctions.NotContainsFunction();
        break;
      case IN:
        routingFunction = new BasicRoutingFunctions.InFunction();
        break;
      case NOT_IN:
        routingFunction = new BasicRoutingFunctions.NotInFunction();
        break;
      case MATCHES:
        routingFunction = new BasicRoutingFunctions.MatchesFunction();
        break;
      case NOT_MATCHES:
        routingFunction = new BasicRoutingFunctions.NotMatchesFunction();
        break;
      default:
        collector.addFailure(
          "Unknown routingFunction " + functionType,
          "Routing function must be one of " + Joiner.on(",").join(RecordRouter.Config.FunctionType.values())
        ).withConfigProperty(RecordRouter.Config.BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
        throw collector.getOrThrowException();
    }
    return routingFunction;
  }

  @Override
  public String toString() {
    return "BasicPortSpecification{" +
      "name='" + getName() + '\'' +
      ", routingFunction=" + routingFunction +
      ", parameter='" + parameter + '\'' +
      '}';
  }
}
