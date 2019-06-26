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
  private final Function function;
  private final String parameter;

  PortSpecification(String name, GenericSplitter.Config.FunctionType functionType, String parameter) {
    this.name = name;
    this.function = fromFunctionType(functionType);
    this.parameter = parameter;
  }

  String getName() {
    return name;
  }

  Function getFunction() {
    return function;
  }

  String getParameter() {
    return parameter;
  }

  private Function fromFunctionType(GenericSplitter.Config.FunctionType functionType) {
    Function function;
    switch (functionType) {
      case EQUALS:
        function = new EqualsFunction();
        break;
      case NOT_EQUALS:
        function = new NotEqualsFunction();
        break;
      case CONTAINS:
        function = new ContainsFunction();
        break;
      case NOT_CONTAINS:
        function = new NotContainsFunction();
        break;
      case IN:
        function = new InFunction();
        break;
      case NOT_IN:
        function = new NotInFunction();
        break;
      default:
        throw new IllegalArgumentException("Unknown function " + functionType);
    }
    return function;
  }

  @Override
  public String toString() {
    return "PortSpecification{" +
      "name='" + name + '\'' +
      ", function=" + function +
      ", parameter='" + parameter + '\'' +
      '}';
  }
}
