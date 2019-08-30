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

  private Map<String, Schema> generateSchemas(Schema inputSchema) {
    Map<String, Schema> schemas = new HashMap<>();
    portSpecifications = config.getPortConfigs();
    // Add all ports from the port config to the schemas
    for (PortSpecification portSpecification : portSpecifications) {
      schemas.put(portSpecification.getName(), inputSchema);
    }
    // If mismatched records need to be sent to their own port, add that port to the schemas
    if (Config.MismatchHandling.MISMATCH_PORT == config.getMismatchHandling()) {
      schemas.put(config.mismatchPort, inputSchema);
    }
    return schemas;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    portSpecifications = config.getPortConfigs();
  }

  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    Object value = input.get(config.fieldToSplitOn);
    // TODO: Handle null values
    /*if (value == null) {
      LOG.trace("Found null value for {}. Emitting to {} port.", config.fieldToSplitOn, config.mismatchHandling);
      emitter.emit(config.mismatchHandling, input);
      return;
    }*/
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
    if (!matched) {
      if (Config.MismatchHandling.MISMATCH_PORT == config.getMismatchHandling()) {
        emitter.emit(config.mismatchPort, input);
      } else if (Config.MismatchHandling.ERROR_PORT == config.getMismatchHandling()) {
        String error = String.format(
          "Record contained unknown value '%s' for field %s", textValue, config.fieldToSplitOn
        );
        InvalidEntry<StructuredRecord> invalid = new InvalidEntry<>(1, error, input);
        emitter.emitError(invalid);
      } else if (Config.MismatchHandling.SKIP == config.getMismatchHandling()) {
        LOG.trace("Skipping record because value {} for field {} did not match any rule in the port specification");
      }
    }
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

    @Name("fieldToSplitOn")
    @Description("Specifies the field to split on")
    @Macro
    private final String fieldToSplitOn;

    @Name("portSpecification")
    @Description("Specifies the rules to split the data as a json map")
    @Macro
    private final String portSpecification;

    @Name("mismatchHandling")
    @Description("Determines the way to handle records whose value for the field to match on doesn't match an of the " +
      "rules defined in the port configuration. Mismatched records can either be skipped, sent to a specific port  " +
      "(in this case the mismatch port should be specified), or sent to an error port. By default, mismatched " +
      "records are skipped.")
    @Nullable
    private final String mismatchHandling;

    @Name("mismatchPort")
    @Description("Determines the port to which records that do not match any of the rules in the port specification " +
      "are routed. This is only used if mismatch handling is set to port, and defaults to 'Other'.")
    @Nullable
    private final String mismatchPort;

    Config(String fieldToSplitOn, String portSpecification, @Nullable String mismatchHandling, @Nullable String mismatchPort) {
      this.fieldToSplitOn = fieldToSplitOn;
      this.portSpecification = portSpecification;
      this.mismatchHandling = mismatchHandling == null ? "Skip" : mismatchHandling;
      this.mismatchPort = mismatchPort == null ? "Other" : mismatchPort;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      if (fieldToSplitOn == null || fieldToSplitOn.isEmpty()) {
        throw new IllegalArgumentException("Field to split on is required.");
      }
      Schema.Field field = inputSchema.getField(fieldToSplitOn);
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
      validatePortConfig(portSpecification);
    }

    private void validatePortConfig(String portConfig) {

    }

    List<PortSpecification> getPortConfigs() {
      List<PortSpecification> portSpecifications = new ArrayList<>();
      if (containsMacro("portSpecification")) {
        return portSpecifications;
      }
      Set<String> portNames = new HashSet<>();
      for (String singlePortConfig : Splitter.on(',').trimResults().split(portSpecification)) {
        int colonIdx = singlePortConfig.indexOf(':');
        if (colonIdx < 0) {
          throw new IllegalArgumentException(String.format(
            "Could not find ':' separating port name from its selection operation in '%s'.", singlePortConfig));
        }
        String portName = singlePortConfig.substring(0, colonIdx).trim();
        if (!portNames.add(portName)) {
          throw new IllegalArgumentException(String.format(
            "Cannot create multiple ports with the same name '%s'.", portName));
        }

        String functionAndParameter = singlePortConfig.substring(colonIdx + 1).trim();
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

    MismatchHandling getMismatchHandling() {
      return MismatchHandling.fromValue(mismatchHandling)
        .orElseThrow(() -> new InvalidConfigPropertyException("Unsupported mismatch handling value: "
                                                                + mismatchHandling, "mismatchHandling"));
    }

    enum FunctionType {
      EQUALS,
      NOT_EQUALS,
      CONTAINS,
      NOT_CONTAINS,
      IN,
      NOT_IN
    }

    enum MismatchHandling {
      SKIP("Skip"),
      ERROR_PORT("Send to error port"),
      MISMATCH_PORT("Send to mismatch port");

      private final String value;

      MismatchHandling(String value) {
        this.value = value;
      }

      public String getValue() {
        return value;
      }

      /**
       * Converts mismatch handling string value into {@link MismatchHandling} enum.
       *
       * @param mismatchValue mismatch handling string value
       * @return mismatch handling type in optional container
       */
      public static Optional<MismatchHandling> fromValue(String mismatchValue) {
        return Stream.of(values())
          .filter(keyType -> keyType.value.equalsIgnoreCase(mismatchValue))
          .findAny();
      }

    }
  }
}

