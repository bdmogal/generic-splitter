/*
 * Copyright © 2017 Cask Data, Inc.
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

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
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

import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("GenericSplitter") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is an generic splitter transform, which sends a record to an appropriate branch based on the " +
  "evaluation of a simple function on the value of one of its fields.")
public class GenericSplitter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(GenericSplitter.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE =  new TypeToken<Map<String, String>>() {}.getType();

  // Usually, you will need a private variable to store the config that was passed to your class
  private final Config config;

  public GenericSplitter(Config config) {
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
    Map<String, String> splitRules = GSON.fromJson(config.splitRules, MAP_STRING_STRING_TYPE);
    Map<String, Schema> schemas = new HashMap<>();
    for (String portName : splitRules.keySet()) {
      schemas.put(portName, inputSchema);
    }

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
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
  }

  @Override
  public void transform(StructuredRecord input, MultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    Object value = input.get(config.fieldToSplit);

  }

  /**
   * Your plugin's configuration class. The fields here will correspond to the fields in the UI for configuring the
   * plugin.
   */
  public static class Config extends PluginConfig {
    @Name("fieldToSplit")
    @Description("Specifies the field to split on")
    @Macro // <- Macro means that the value will be substituted at runtime by the user.
    private final String fieldToSplit;

    @Name("splitRules")
    @Description("Specifies the rules to split the data as a json map")
    @Macro
    private final String splitRules;

    public Config(String fieldToSplit, String splitRules) {
      this.fieldToSplit = fieldToSplit;
      this.splitRules = splitRules;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      if (fieldToSplit == null || fieldToSplit.isEmpty()) {
        throw new IllegalArgumentException("Field to split on is required.");
      }
      Schema.Field field = inputSchema.getField(fieldToSplit);
      if (field == null) {
        throw new IllegalArgumentException("Field to split on must be present in the input schema");
      }
      Schema fieldSchema = field.getSchema();
      if (!fieldSchema.isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException(
          String.format("Field to split must be a simple type - STRING, INTEGER, FLOAT, LONG, DOUBLE, BOOLEAN. " +
                          "Found '%s'", fieldSchema));
      }
      Schema.Type nonNullableType = fieldSchema.getNonNullable().getType();
      if (splitRules == null || splitRules.isEmpty()) {
        throw new IllegalArgumentException("At least 1 rule to split data must be specified.");
      }
      Map<String, String> rules;
      try {
        rules = GSON.fromJson(splitRules, MAP_STRING_STRING_TYPE);
      } catch(JsonSyntaxException ex) {
        throw new IllegalArgumentException("Split rules must be a valid JSON");
      }
      validateRules(rules);
    }

    private void validateRules(Map<String, String> rules) {

    }
  }
}

