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
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("RecordRouter")
@Description("Routes a record to an appropriate port based on the evaluation of a simple function on the value of " +
  "one of its fields.")
public class RecordRouter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordRouter.class);
  private static final String BASIC_MODE_NAME = "basic";
  private static final String JEXL_MODE_NAME = "jexl";

  private final Config config;
  private PortSpecificationEvaluator evaluator;

  RecordRouter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    MultiOutputStageConfigurer stageConfigurer = configurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    evaluator = getPortSpecificationEvaluator(failureCollector);
    config.validate(inputSchema, failureCollector);
    Map<String, Schema> schemas = generateSchemas(inputSchema);
    stageConfigurer.setOutputSchemas(schemas);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    evaluator = getPortSpecificationEvaluator(context.getFailureCollector());
  }

  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    // Emit based on the specified rules in the port specification
    String port = evaluator.getPort(input);
    // if the port is null, which means the value does not match any rule in the port specification, emit it as a
    // defaulting value, based on the selected default handling option
    if (port == null) {
      emitDefaultValue(input, emitter);
    }
    // if a port is found, emit to the port
    emitter.emit(port, input);
  }

  private PortSpecificationEvaluator getPortSpecificationEvaluator(FailureCollector collector) {
    if (BASIC_MODE_NAME.equals(config.routeSpecificationMode)) {
      return new BasicPortSpecificationEvaluator(config.routingField, config.nullPort, config.portSpecification,
                                                 collector);
    }
    if (JEXL_MODE_NAME.equals(config.routeSpecificationMode)) {
      return new JexlSpecificationEvaluator(config.jexlPortSpecification, collector);
    }
    collector.addFailure(
      String.format("Invalid route specification mode '%s'. Must be one of '%s' or '%s'",
                    config.routeSpecificationMode, BASIC_MODE_NAME, JEXL_MODE_NAME), null
    ).withConfigProperty("routeSpecificationMode");
    throw collector.getOrThrowException();
  }

  private void emitDefaultValue(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    if (Config.DefaultHandling.DEFAULT_PORT == config.getDefaultHandling()) {
      emitter.emit(config.defaultPort, input);
    } else if (Config.DefaultHandling.ERROR_PORT == config.getDefaultHandling()) {
      String error = String.format(
        "Record contained unknown value for field %s", config.routingField
      );
      InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(1, error, input);
      emitter.emitError(invalid);
    } else if (Config.DefaultHandling.SKIP == config.getDefaultHandling()) {
      LOG.trace("Skipping record because value for field {} did not match any rule in the port specification",
                config.routingField);
    }
  }

  private Map<String, Schema> generateSchemas(Schema inputSchema) {
    Map<String, Schema> schemas = new HashMap<>();
    // Add all ports from the port config to the schemas
    for (String port : evaluator.getAllPorts()) {
      schemas.put(port, inputSchema);
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

    private static final String ROUTE_SPECIFICATION_MODE_PROPERTY_NAME = "routeSpecificationMode";
    private static final String ROUTING_FIELD_PROPERTY_NAME = "routingField";
    static final String BASIC_PORT_SPECIFICATION_PROPERTY_NAME = "portSpecification";
    static final String JEXL_PORT_SPECIFICATION_PROPERTY_NAME = "jexlPortSpecification";
    private static final String DEFAULT_HANDLING_PROPERTY_NAME = "defaultHandling";

    @Name(ROUTE_SPECIFICATION_MODE_PROPERTY_NAME)
    @Description("The mode in which you would like to provide the routing specification. The basic mode allows you " +
      "to specify multiple simple routing rules, where each rule operates on a single field in the input schema. " +
      "For basic, the Routing Field and Port Specification are required. The jexl mode allows you to specify " +
      "complex routing rules, which can operate on multiple input fields in a single rule. In the jexl mode, " +
      "you can use JEXL expressions to specify the routing configuration. Also specify the JEXL Port Configuration " +
      "for the jexl mode. Defaults to basic.")
    @Macro
    private final String routeSpecificationMode;

    @Name(ROUTING_FIELD_PROPERTY_NAME)
    @Description("Specifies the field in the input schema on which the rules in the _Port Specification_ should be " +
      "applied, to determine the port where the record should be routed to.")
    @Macro
    @Nullable
    private final String routingField;

    @Name(BASIC_PORT_SPECIFICATION_PROPERTY_NAME)
    @Description("Specifies the rules to determine the port where the record should be routed to. Rules are applied " +
      "on the value of the routing field. The port specification is expressed as a comma-separated list of rules, " +
      "where each rule has the format [port-name]:[function-name]([parameter-name]). [port-name] is the name of the " +
      "port to route the record to if the rule is satisfied. [function-name] can be one of equals, not_equals, " +
      "contains, not_contains, in, not_in, matches, not_matches. [parameter-name] is the parameter based on which " +
      "the selected function evaluates the value of the routing field.")
    @Macro
    @Nullable
    private final String portSpecification;

    @Name(JEXL_PORT_SPECIFICATION_PROPERTY_NAME)
    @Description("Specifies a '#' separated list of ports, and the JEXL expression to route the record to the port " +
      "in the format [port-name]:[jexl-expression]. All the input fields are available as variables in the JEXL " +
      "expression. Additionally, utility methods from common Java classes such as Math, Guava Strings, Apache " +
      "Commons Lang StringUtils, Bytes and Arrays are also available for use in the port specification rules. To " +
      "avoid conflicts with delimiters, it is recommended to URL-encode the JEXL expressions. Some example " +
      "expressions: PortA:stringutils%3AstartsWith(part_id%2C'a'), PortB:stringutils%3Acontains(supplier%2C'abc'), " +
      "PortC:math%3Aceil(amount)")
    @Macro
    @Nullable
    private final String jexlPortSpecification;

    @Name(DEFAULT_HANDLING_PROPERTY_NAME)
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

    @Name("nullPort")
    @Description("Determines the port to which records with null values for the field to split on are routed to. " +
      "This is only used if default handling is set to NULL_PORT, and defaults to 'Null'.")
    @Nullable
    private final String nullPort;

    Config(String routeSpecificationMode, @Nullable String routingField, @Nullable String portSpecification,
           @Nullable String jexlPortSpecification, @Nullable String defaultHandling, @Nullable String defaultPort,
           @Nullable String nullPort) {
      this.routeSpecificationMode = routeSpecificationMode;
      this.routingField = routingField;
      this.jexlPortSpecification = jexlPortSpecification;
      this.portSpecification = portSpecification;
      this.defaultHandling = defaultHandling == null ? DefaultHandling.DEFAULT_PORT.value : defaultHandling;
      this.defaultPort = defaultPort == null ? DEFAULT_PORT_NAME : defaultPort;
      this.nullPort = nullPort == null ? DEFAULT_NULL_PORT_NAME : nullPort;
    }

    private void validate(Schema inputSchema, FailureCollector collector) throws IllegalArgumentException {
      if (!BASIC_MODE_NAME.equals(routeSpecificationMode) && !JEXL_MODE_NAME.equals(routeSpecificationMode)) {
        collector.addFailure(String.format("Unknown route specification mode %s", routeSpecificationMode),
                             String.format("Please specify one of %s or %s", BASIC_MODE_NAME, JEXL_MODE_NAME))
          .withConfigProperty(ROUTE_SPECIFICATION_MODE_PROPERTY_NAME);
      }
      Optional<DefaultHandling> handling = DefaultHandling.fromValue(defaultHandling);
      if (!handling.isPresent()) {
        collector.addFailure(String.format("Unsupported default handling value %s", DEFAULT_HANDLING_PROPERTY_NAME),
                             String.format("Please specify one of %s", Joiner.on(",").join(DefaultHandling.values())))
          .withConfigProperty(DEFAULT_HANDLING_PROPERTY_NAME);
      }

      if (JEXL_MODE_NAME.equals(routeSpecificationMode) && Strings.isNullOrEmpty(jexlPortSpecification)) {
        collector.addFailure("In jexl mode, Jexl port specification must be provided", null)
          .withConfigProperty(JEXL_PORT_SPECIFICATION_PROPERTY_NAME);
        return;
      }

      if (routingField == null || routingField.isEmpty()) {
        collector.addFailure("Field to split is required when routing mode is basic.",
                             "Please provide the field to split on").withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
      }
      Schema.Field field = inputSchema.getField(routingField);
      if (field == null) {
        collector.addFailure(String.format("Routing field %s not found in the input schema", routingField),
                             "Please provide a field that exists in the input schema")
          .withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
        throw collector.getOrThrowException();
      }
      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!ALLOWED_TYPES.contains(type)) {
        collector.addFailure(
          String.format("Field to split must be one of - STRING, INTEGER, LONG, FLOAT, DOUBLE, BOOLEAN. " +
                          "Found '%s'", fieldSchema), null).withConfigProperty(ROUTING_FIELD_PROPERTY_NAME);
      }
      if (portSpecification == null || portSpecification.isEmpty()) {
        collector.addFailure("No port specifications defined.", "Please provide at least 1 port specification")
          .withConfigProperty(BASIC_PORT_SPECIFICATION_PROPERTY_NAME);
      }
    }

    DefaultHandling getDefaultHandling() {
      return DefaultHandling.fromValue(defaultHandling)
        .orElseThrow(() -> new IllegalArgumentException("Unsupported default handling value: " + defaultHandling));
    }

    enum FunctionType {
      EQUALS,
      NOT_EQUALS,
      CONTAINS,
      NOT_CONTAINS,
      IN,
      NOT_IN,
      MATCHES,
      NOT_MATCHES
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
  }
}

