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

package io.cdap.plugin.generic.splitter;

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
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 *
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name("GenericSplitter") // <- NOTE: The name of the plugin should match the name of the docs and widget json files.
@Description("This is an example transform.")
public class ExampleTransformPlugin extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(ExampleTransformPlugin.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE =  new TypeToken<Map<String, String>>() {}.getType();

  // Usually, you will need a private variable to store the config that was passed to your class
  private final Config config;
  private Schema outputSchema;

  public ExampleTransformPlugin(Config config) {
    this.config = config;
  }

  /**
   * This function is called when the pipeline is published. You should use this for validating the config and setting
   * additional parameters in pipelineConfigurer.getStageConfigurer(). Those parameters will be stored and will be made
   * available to your plugin during runtime via the TransformContext. Any errors thrown here will stop the pipeline
   * from being published.
   * @param pipelineConfigurer Configures an ETL Pipeline. Allows adding datasets and streams and storing parameters
   * @throws IllegalArgumentException If the config is invalid.
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // It's usually a good idea to validate the configuration at this point. It will stop the pipeline from being
    // published if this throws an error.
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Output schema cannot be parsed.", e);
    }
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
    outputSchema = Schema.parseJson(config.schema);
  }

  /**
   * This is the method that is called for every record in the pipeline and allows you to make any transformations
   * you need and emit one or more records to the next stage.
   * @param input The record that is coming into the plugin
   * @param emitter An emitter allowing you to emit one or more records to the next stage
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // Get all the fields that are in the output schema
    List<Schema.Field> fields = outputSchema.getFields();
    // Create a builder for creating the output record
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    // Add all the values to the builder
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (input.get(name) != null) {
        builder.set(name, input.get(name));
      }
    }
    // If you wanted to make additional changes to the output record, this might be a good place to do it.

    // Finally, build and emit the record.
    emitter.emit(builder.build());
  }

  /**
   * This function will be called at the end of the pipeline. You can use it to clean up any variables or connections.
   */
  @Override
  public void destroy() {
    // No Op
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

    @Name("schema")
    @Description("")
    private final String schema;

    public Config(String fieldToSplit, String splitRules, String schema) {
      this.fieldToSplit = fieldToSplit;
      this.splitRules = splitRules;
      this.schema = schema;
    }

    private void validate(Schema inputSchema) throws IllegalArgumentException {
      Schema parsedSchema;
      try {
        parsedSchema = Schema.parseJson(inputSchema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Output schema cannot be parsed.", e);
      }
      if (fieldToSplit == null || fieldToSplit.isEmpty()) {
        throw new IllegalArgumentException("Field to split on is required.");
      }
      if (parsedSchema.getField(fieldToSplit) == null) {
        throw new IllegalArgumentException("Field to split on must be present in the input schema");
      }
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

