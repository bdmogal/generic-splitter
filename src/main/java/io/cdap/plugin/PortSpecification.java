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

class PortSpecification {
  private final String name;
  private final RoutingFunction routingFunction;
  private final String parameter;

  PortSpecification(String name, RecordRouter.Config.FunctionType functionType, String parameter) {
    this.name = name;
    this.routingFunction = fromFunctionType(functionType);
    this.parameter = parameter;
  }

  String getName() {
    return name;
  }

  RoutingFunction getRoutingFunction() {
    return routingFunction;
  }

  String getParameter() {
    return parameter;
  }

  private RoutingFunction fromFunctionType(RecordRouter.Config.FunctionType functionType) {
    RoutingFunction routingFunction;
    switch (functionType) {
      case EQUALS:
        routingFunction = new RoutingFunctions.EqualsFunction();
        break;
      case NOT_EQUALS:
        routingFunction = new RoutingFunctions.NotEqualsFunction();
        break;
      case CONTAINS:
        routingFunction = new RoutingFunctions.ContainsFunction();
        break;
      case NOT_CONTAINS:
        routingFunction = new RoutingFunctions.NotContainsFunction();
        break;
      case IN:
        routingFunction = new RoutingFunctions.InFunction();
        break;
      case NOT_IN:
        routingFunction = new RoutingFunctions.NotInFunction();
        break;
      default:
        throw new IllegalArgumentException("Unknown routingFunction " + functionType);
    }
    return routingFunction;
  }

  @Override
  public String toString() {
    return "PortSpecification{" +
      "name='" + name + '\'' +
      ", routingFunction=" + routingFunction +
      ", parameter='" + parameter + '\'' +
      '}';
  }
}
