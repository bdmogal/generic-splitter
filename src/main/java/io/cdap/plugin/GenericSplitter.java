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
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 *
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("GenericSplitter") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is an generic splitter transform, which sends a record to an appropriate branch based on the " +
  "evaluation of a simple function on the value of one of its fields.")
public class GenericSplitter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GenericSplitter.class);

  private final Config config;
  private List<PortConfig> portConfigs;

  GenericSplitter(Config config) {
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
    portConfigs = config.getPortConfigs();
    for (PortConfig portConfig : portConfigs) {
      schemas.put(portConfig.getName(), inputSchema);
    }
    schemas.put(config.nullPort, inputSchema);
    return schemas;
  }

  /**
   * This function is called when the pipeline has started. The values configured in here will be made available to the
   * transform function. Use this for initializing costly objects and opening connections that will be reused.
   * @param context Context for a pipeline stage, providing access to information about the stage, metrics, and plugins.
   * @throws Exception If there are any issues before starting the pipeline.
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    portConfigs = config.getPortConfigs();
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) {
    Object value = input.get(config.fieldToSplitOn);
    if (value == null) {
      LOG.trace("Found null value for {}. Emitting to {} port.", config.fieldToSplitOn, config.nullPort);
      emitter.emit(config.nullPort, input);
      return;
    }
    String textValue = String.valueOf(value);
    for (PortConfig portConfig : portConfigs) {
      String portName = portConfig.getName();
      Function function = portConfig.getFunction();
      if (function.evaluate(textValue)) {
        emitter.emit(portName, input);
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

    @Name("nullPort")
    @Description("Determines the port name where records that contain a null value for the field to split on " +
      "are sent. Defaults to Null.")
    @Nullable
    private final String nullPort;

    @Name("portConfig")
    @Description("Specifies the rules to split the data as a json map")
    @Macro
    private final String portConfig;

    Config(String fieldToSplitOn, @Nullable String nullPort, String portConfig) {
      this.fieldToSplitOn = fieldToSplitOn;
      this.nullPort = nullPort == null ? "Null" : nullPort;
      this.portConfig = portConfig;
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
      if (portConfig == null || portConfig.isEmpty()) {
        throw new IllegalArgumentException("At least 1 port config must be specified.");
      }
      validatePortConfig(portConfig);
    }

    private void validatePortConfig(String portConfig) {

    }

    List<PortConfig> getPortConfigs() {
      List<PortConfig> portConfigs = new ArrayList<>();
      if (containsMacro("portConfig")) {
        return portConfigs;
      }
      Set<String> portNames = new HashSet<>();
      for (String singlePortConfig : Splitter.on(',').trimResults().split(portConfig)) {
        int colonIdx = singlePortConfig.indexOf(':');
        if (colonIdx < 0) {
          throw new IllegalArgumentException(String.format(
            "Could not find ':' separating port name from its selection operation in '%s'.", singlePortConfig));
        }
        String name = singlePortConfig.substring(0, colonIdx).trim();
        if (!portNames.add(name)) {
          throw new IllegalArgumentException(String.format(
            "Cannot create multiple ports with the same name '%s'.", name));
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

        portConfigs.add(new PortConfig(name, function, parameter));
      }

      if (portConfigs.isEmpty()) {
        throw new IllegalArgumentException("The 'portConfigs' property must be set.");
      }
      return portConfigs;
    }

    enum FunctionType {
      EQUALS,
      NOT_EQUALS,
      CONTAINS,
      NOT_CONTAINS,
      IN,
      NOT_IN
    }
  }
}

