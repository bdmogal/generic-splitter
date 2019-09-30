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
import java.util.List;
import javax.annotation.Nullable;

/**
 * Interface that defines the contract for evaluating a port specification. It validates the port specification and
 * determines the port to which a record should be routed based on the port specification.
 */
public interface PortSpecificationEvaluator {
  /**
   * Validate the port specification string supplied by a user
   */
  void validate();

  /**
   * Returns the names of all the ports in the port specification
   *
   * @return the port names from the port specification
   */
  List<String> getAllPorts();

  /**
   * Determines the port that a record should be routed to, based on the port specification
   *
   * @param record the input record to route
   * @return the port to route the record to
   */
  @Nullable
  String getPort(StructuredRecord record);
}
