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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("RecordRouter")
@Description("This is an generic splitter transform, which sends a record to an appropriate branch based on the " +
  "evaluation of a simple function on the value of one of its fields.")
public class RecordRouter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordRouter.class);

  private final Config config;
  private List<PortSpecification> portSpecifications;

  RecordRouter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    Schema inputSchema = configurer.getMultiOutputStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    Map<String, Schema> schemas = generateSchemas(inputSchema);
    configurer.getMultiOutputStageConfigurer().setOutputSchemas(schemas);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    portSpecifications = config.getPortSpecification();
  }

  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    Object value = input.get(config.routingField);
    // if the value is null, emit it based on the selected null handling option
    if (value == null) {
      LOG.trace("Found null value for {}.", config.routingField);
      emitNullValue(input, emitter);
      return;
    }
    // if the value is not null, emit it based on the specified rules in the port specification
    String textValue = String.valueOf(value);
    boolean matched = false;
    for (PortSpecification portSpecification : portSpecifications) {
      String portName = portSpecification.getName();
      RoutingFunction routingFunction = portSpecification.getRoutingFunction();
      if (routingFunction.evaluate(textValue, portSpecification.getParameter())) {
        matched = true;
        emitter.emit(portName, input);
      }
    }
    // if the value does not match any rule in the port specification, emit it as a defaulting value, based on the
    // selected default handling option
    if (!matched) {
      emitDefaultValue(input, emitter, textValue);
    }
  }

  private void emitNullValue(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    if (Config.NullHandling.NULL_PORT == config.getNullHandling()) {
      emitter.emit(config.nullPort, input);
    } else if (Config.NullHandling.ERROR_PORT == config.getNullHandling()) {
      String error = String.format(
        "Record contained null value for field %s", config.routingField
      );
      InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(1, error, input);
      emitter.emitError(invalid);
    } else if (Config.DefaultHandling.SKIP == config.getDefaultHandling()) {
      LOG.trace("Skipping record because field {} has a null value", config.routingField);
    }
  }

  private void emitDefaultValue(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter,
                                String defaultValue) {
    if (Config.DefaultHandling.DEFAULT_PORT == config.getDefaultHandling()) {
      emitter.emit(config.defaultPort, input);
    } else if (Config.DefaultHandling.ERROR_PORT == config.getDefaultHandling()) {
      String error = String.format(
        "Record contained unknown value '%s' for field %s", defaultValue, config.routingField
      );
      InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(1, error, input);
      emitter.emitError(invalid);
    } else if (Config.DefaultHandling.SKIP == config.getDefaultHandling()) {
      LOG.trace("Skipping record because value {} for field {} did not match any rule in the port specification",
                defaultValue, config.routingField);
    }
  }

  private Map<String, Schema> generateSchemas(Schema inputSchema) {
    Map<String, Schema> schemas = new HashMap<>();
    portSpecifications = config.getPortSpecification();
    // Add all ports from the port config to the schemas
    for (PortSpecification portSpecification : portSpecifications) {
      schemas.put(portSpecification.getName(), inputSchema);
    }
    // If defaulting records need to be sent to their own port, add that port to the schemas
    if (Config.DefaultHandling.DEFAULT_PORT == config.getDefaultHandling()) {
      schemas.put(config.defaultPort, inputSchema);
    }
    return schemas;
  }

  /**
   * Your plugin's configuration class. The fields here will correspond to the fields in the UI for configuring the
   * plugin.
   */
  static class Config extends PluginConfig {
    private static final List<Schema.Type> ALLOWED_TYPES = new ArrayList<>();
    static {
      ALLOWED_TYPES.add(Schema.Type.STRING);
      ALLOWED_TYPES.add(Schema.Type.INT);
      ALLOWED_TYPES.add(Schema.Type.LONG);
      ALLOWED_TYPES.add(Schema.Type.FLOAT);
      ALLOWED_TYPES.add(Schema.Type.DOUBLE);
      ALLOWED_TYPES.add(Schema.Type.BOOLEAN);
    }
    @VisibleForTesting
    static final String DEFAULT_PORT_NAME = "Default";
    @VisibleForTesting
    static final String DEFAULT_NULL_PORT_NAME = "Null";

    @Name("routingField")
    @Description("Specifies the field on which the port specification rules should be applied, to determine the " +
      "port where the record should be routed to.")
    @Macro
    private final String routingField;

    @Name("portSpecification")
    @Description("Specifies the rules to determine the port where the record should be routed to. Rules are applied " +
      "on the value of the routing field. The port specification is expressed as a comma-separated list of rules, " +
      "where each rule has the format [port-name]:[function-name]([parameter-name]). [port-name] is the name of the " +
      "port to route the record to if the rule is satisfied. [function-name] can be one of equals, not_equals, " +
      "contains, not_contains, in, not_in. [parameter-name] is the parameter based on which the selected function " +
      "evaluates the value of the routing field.")
    @Macro
    private final String portSpecification;

    @Name("defaultHandling")
    @Description("Determines the way to handle records whose value for the field to match on doesn't match an of the " +
      "rules defined in the port configuration. Defaulting records can either be skipped, sent to a specific port  " +
      "(in which case the default port should be specified), or sent to an error port. By default, defaulting " +
      "records are skipped.")
    @Nullable
    private final String defaultHandling;

    @Name("defaultPort")
    @Description("Determines the port to which records that do not match any of the rules in the port specification " +
      "are routed. This is only used if default handling is set to port, and defaults to 'Default'.")
    @Nullable
    private final String defaultPort;

    @Name("nullHandling")
    @Description("Determines the way to handle records with null values for the field to split on. Such records " +
      "can either be skipped, sent to a specific port  (in which case the null port should be specified), or sent " +
      "to an error port. By default, null records are sent to the null port.")
    @Nullable
    private final String nullHandling;

    @Name("nullPort")
    @Description("Determines the port to which records with null values for the field to split on are routed to. " +
      "This is only used if default handling is set to NULL_PORT, and defaults to 'Null'.")
    @Nullable
    private final String nullPort;

    Config(String routingField, String portSpecification, @Nullable String defaultHandling,
           @Nullable String defaultPort, @Nullable String nullHandling, @Nullable String nullPort) {
      this.routingField = routingField;
      this.portSpecification = portSpecification;
      this.defaultHandling = defaultHandling == null ? DefaultHandling.DEFAULT_PORT.name() : defaultHandling;
      this.defaultPort = defaultPort == null ? DEFAULT_PORT_NAME : defaultPort;
      this.nullHandling = nullHandling == null ? NullHandling.NULL_PORT.name() : nullHandling;
      this.nullPort = nullPort == null ? DEFAULT_NULL_PORT_NAME : nullPort;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      if (routingField == null || routingField.isEmpty()) {
        throw new IllegalArgumentException("Field to split on is required.");
      }
      Schema.Field field = inputSchema.getField(routingField);
      if (field == null) {
        throw new IllegalArgumentException("Field to split on must be present in the input schema");
      }
      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!ALLOWED_TYPES.contains(type)) {
        throw new IllegalArgumentException(
          String.format("Field to split must be one of - STRING, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN. " +
                          "Found '%s'", fieldSchema));
      }
      if (portSpecification == null || portSpecification.isEmpty()) {
        throw new IllegalArgumentException("At least 1 port config must be specified.");
      }
      validatePortSpecification(portSpecification);
    }

    private void validatePortSpecification(String portSpecification) {

    }

    List<PortSpecification> getPortSpecification() {
      List<PortSpecification> portSpecifications = new ArrayList<>();
      if (containsMacro("portSpecification")) {
        return portSpecifications;
      }
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
        FunctionType function;
        try {
          function = FunctionType.valueOf(functionStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException(String.format(
            "Invalid function '%s'. Must be one of %s.", functionStr, Joiner.on(',').join(FunctionType.values())));
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
        portSpecifications.add(new PortSpecification(portName, function, parameter));
      }

      if (portSpecifications.isEmpty()) {
        throw new IllegalArgumentException("The 'portSpecifications' property must be set.");
      }
      return portSpecifications;
    }

    DefaultHandling getDefaultHandling() {
      return DefaultHandling.fromValue(defaultHandling)
        .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported default handling value: "
                                                                + defaultHandling, "defaultHandling"));
    }

    NullHandling getNullHandling() {
      return NullHandling.fromValue(nullHandling)
        .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported null handling value: "
                                                                + nullHandling, "nullHandling"));
    }

    enum FunctionType {
      EQUALS,
      NOT_EQUALS,
      CONTAINS,
      NOT_CONTAINS,
      IN,
      NOT_IN
    }

    enum DefaultHandling {
      SKIP("Skip"),
      ERROR_PORT("Send to error port"),
      DEFAULT_PORT("Send to default port");

      private final String value;

      DefaultHandling(String value) {
        this.value = value;
      }

      /**
       * Converts default handling string value into {@link DefaultHandling} enum.
       *
       * @param defaultValue default handling string value
       * @return default handling type in optional container
       */
      public static Optional<DefaultHandling> fromValue(String defaultValue) {
        return Stream.of(values())
          .filter(keyType -> keyType.value.equalsIgnoreCase(defaultValue))
          .findAny();
      }
    }

    enum NullHandling {
      SKIP("Skip"),
      ERROR_PORT("Send to error port"),
      NULL_PORT("Send to null port");

      private final String value;

      NullHandling(String value) {
        this.value = value;
      }

      /**
       * Converts null handling string value into {@link NullHandling} enum.
       *
       * @param nullValue default handling string value
       * @return default handling type in optional container
       */
      public static Optional<NullHandling> fromValue(String nullValue) {
        return Stream.of(values())
          .filter(keyType -> keyType.value.equalsIgnoreCase(nullValue))
          .findAny();
      }
    }
  }
}

